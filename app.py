\
from __future__ import annotations

import os, io, html
from pathlib import Path
from typing import List, Dict

from shiny import App, ui, reactive, render, session as shiny_session

from db import get_engine, init_db, upsert_document, get_document, find_document_by_name, list_codes, create_code, insert_segment, list_segments

def secure_filename(name: str) -> str:
    import os, re
    name = name.replace("\\", "/").split("/")[-1]
    name = re.sub(r'[^A-Za-z0-9._-]+', '_', name)
    if name in ('', '.', '..'):
        name = 'upload.txt'
    return name[:200]

#
# --- Setup DB engine and ensure schema exists at startup ---
#
engine = get_engine()
init_db(engine)

#
# --- Helpers ---
#
def highlight_text(text: str, segments: List[Dict]) -> str:
    """Return HTML with <mark> wrapped around coded segments using raw offsets."""
    if not segments:
        return html.escape(text)

    parts = []
    last = 0
    for seg in sorted(segments, key=lambda s: s["start_offset"]):
        s = max(0, seg["start_offset"])
        e = max(s, seg["end_offset"])
        # clamp
        s = min(s, len(text))
        e = min(e, len(text))
        if s > last:
            parts.append(html.escape(text[last:s]))
        parts.append("<mark>" + html.escape(text[s:e]) + "</mark>")
        last = e
    if last < len(text):
        parts.append(html.escape(text[last:]))
    return "".join(parts)

def sniff_text(file_path: Path, filename: str) -> str:
    # Phase 0: treat as plaintext; ignore binary docs for now
    return Path(file_path).read_text(errors="ignore")

#
# --- UI ---
#
app_ui = ui.page_fluid(
    ui.h3("Mini QDA — Phase 0"),
    ui.layout_sidebar(
        ui.panel_sidebar(
            ui.input_file("file", "Upload a .txt file", accept=[".txt"]),
            ui.input_text("new_code", "New code name"),
            ui.input_action_button("add_code", "Add code"),
            ui.input_select("code", "Apply code", choices=[]),
            ui.input_action_button("apply", "Apply to selection"),
            ui.tags.hr(),
            ui.input_text("open_filename", "Reload by filename (exact)"),
            ui.input_action_button("open", "Open document"),
        ),
        ui.panel_main(
            ui.tags.div(
                {"id": "docview", "style": "white-space:pre-wrap; border:1px solid #ddd; padding:1rem; min-height:300px"},
                "Upload a file to begin."
            ),
            ui.tags.div({"id":"selinfo", "style":"margin-top:0.5rem; color:#666;"}),
        )
    ),
    # JS: capture selection within #docview and send offsets + text
    ui.tags.script("""
      (function(){
        const box = document.getElementById('docview');
        function getSelectionIn(el){
          const sel = window.getSelection();
          if (!sel || sel.isCollapsed) return null;
          // Only proceed if selection is inside our box
          if (!el.contains(sel.anchorNode) || !el.contains(sel.focusNode)) return null;
          const text = el.innerText;
          const selected = sel.toString();
          if (!selected) return null;
          const idx = text.indexOf(selected);
          if (idx < 0) return null;
          return { start: idx, end: idx + selected.length, text: selected };
        }
        document.addEventListener('mouseup', () => {
          const payload = getSelectionIn(box);
          if (payload) {
            // priority: 'event' ensures reactivity triggers
            Shiny.setInputValue('selection', payload, {priority: 'event'});
          }
        });
      })();
    """)
)

#
# --- Server ---
#
def server(input, output, session: shiny_session.Session):
    current_doc_id = reactive.Value(None)
    current_text = reactive.Value("")

    def refresh_codes():
        opts = [{"label": c["name"], "value": str(c["id"])} for c in list_codes(engine)]
        session.send_input_message("code", {"options": opts})

    @reactive.effect
    def _init():
        refresh_codes()

    @reactive.effect
    @reactive.event(input.add_code)
    def _add_code():
        name = (input.new_code() or "").strip()
        if not name:
            return
        create_code(engine, name)
        refresh_codes()
        session.send_input_message("new_code", {"value": ""})

    @reactive.effect
    @reactive.event(input.file)
    def _on_upload():
        f = input.file()
        if not f:
            return
        p = Path(f[0]["datapath"])
        filename = secure_filename(f[0]["name"])
        text = sniff_text(p, filename)
        doc_id = upsert_document(engine, filename, text)
        current_doc_id.set(doc_id)
        current_text.set(text)
        _render()

    @reactive.effect
    @reactive.event(input.open)
    def _open_existing():
        name = (input.open_filename() or "").strip()
        if not name:
            return
        doc = find_document_by_name(engine, name)
        if doc:
            current_doc_id.set(doc["id"])
            current_text.set(doc["content"])
            _render()
        else:
            session.run_js(f"document.getElementById('docview').innerText = 'Not found: {html.escape(name)}'")

    @reactive.effect
    @reactive.event(input.apply)
    def _apply_code():
        sel = input.selection()
        code_id = input.code()
        doc_id = current_doc_id.get()
        if not sel or not code_id or not doc_id:
            return
        insert_segment(engine, int(doc_id), int(code_id), int(sel["start"]), int(sel["end"]), sel["text"])
        _render()  # re-highlight

    def _render():
        # fetch segments and build HTML
        doc_id = current_doc_id.get()
        text = current_text.get()
        if not doc_id or text is None:
            return
        segments = list_segments(engine, int(doc_id))
        html_content = highlight_text(text, segments)
        session.run_js(f"document.getElementById('docview').innerHTML = {html_content!r};")
        # Show selection info (last selection)
        sel = input.selection()
        if sel:
            info = f"Selected {sel['start']}–{sel['end']} ({len(sel['text'])} chars)"
            session.run_js(f"document.getElementById('selinfo').innerText = {info!r};")

app = App(app_ui, server)


if __name__ == "__main__":
    from shiny import run_app
    run_app(app, host="127.0.0.1", port=8000)
