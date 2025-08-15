from __future__ import annotations

import os, io, html, chardet
from pathlib import Path
from typing import List, Dict

from shiny import App, ui, reactive, render, session as shiny_session

from db import get_engine, init_db, upsert_document, get_document, find_document_by_name, list_codes, create_code, insert_segment, list_segments, get_document_preview

# Configuration constants
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB limit
MAX_PREVIEW_SIZE = 1024 * 1024    # 1MB for in-memory preview
CHUNK_SIZE = 8192                 # For reading files in chunks

def secure_filename(name: str) -> str:
    import os, re
    name = name.replace("\\", "/").split("/")[-1]
    name = re.sub(r'[^A-Za-z0-9._-]+', '_', name)
    if name in ('', '.', '..'):
        name = 'upload.txt'
    return name[:200]

def detect_encoding_and_read(file_path: Path, max_detection_bytes: int = 10000) -> tuple[str, str]:
    """
    Detect file encoding and read content safely.
    Returns (content, detected_encoding)
    """
    # Read a sample for encoding detection
    with open(file_path, 'rb') as f:
        raw_sample = f.read(max_detection_bytes)
    
    # Detect encoding
    detection_result = chardet.detect(raw_sample)
    detected_encoding = detection_result['encoding'] or 'utf-8'
    confidence = detection_result.get('confidence', 0)
    
    # If confidence is low, fall back to common encodings
    encodings_to_try = [detected_encoding, 'utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    
    content = None
    used_encoding = None
    
    for encoding in encodings_to_try:
        try:
            content = file_path.read_text(encoding=encoding)
            used_encoding = encoding
            break
        except (UnicodeDecodeError, LookupError):
            continue
    
    if content is None:
        # Last resort: read as utf-8 with replacement
        content = file_path.read_text(encoding='utf-8', errors='replace')
        used_encoding = 'utf-8 (with replacements)'
    
    return content, used_encoding

def validate_file_size(file_path: Path, max_size: int = MAX_FILE_SIZE) -> None:
    """Validate file size before processing."""
    size = file_path.stat().st_size
    if size > max_size:
        raise ValueError(f"File too large: {size:,} bytes (max: {max_size:,} bytes)")

def sniff_text(file_path: Path, filename: str) -> tuple[str, dict]:
    """
    Read and validate text file with proper encoding detection.
    Returns (content, metadata)
    """
    validate_file_size(file_path)
    content, encoding = detect_encoding_and_read(file_path)
    
    metadata = {
        'encoding': encoding,
        'size_bytes': file_path.stat().st_size,
        'char_count': len(content),
        'line_count': content.count('\n') + 1
    }
    
    return content, metadata

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

    # Sort segments by start position and handle overlaps
    sorted_segments = sorted(segments, key=lambda s: (s["start_offset"], s["end_offset"]))
    
    parts = []
    last = 0
    
    for seg in sorted_segments:
        s = max(0, min(seg["start_offset"], len(text)))
        e = max(s, min(seg["end_offset"], len(text)))
        
        # Skip if this segment overlaps with already processed text
        if s < last:
            continue
            
        # Add text before this segment
        if s > last:
            parts.append(html.escape(text[last:s]))
        
        # Add highlighted segment with code info as title
        code_id = seg.get("code_id", "")
        title_attr = f'title="Code ID: {code_id}"' if code_id else ''
        parts.append(f"<mark {title_attr}>" + html.escape(text[s:e]) + "</mark>")
        last = e
    
    # Add remaining text
    if last < len(text):
        parts.append(html.escape(text[last:]))
    
    return "".join(parts)

def get_text_for_display(doc_id: int) -> tuple[str, bool]:
    """
    Get text for display, handling large documents by showing preview.
    Returns (text_content, is_preview)
    """
    try:
        # First try to get full document
        doc = get_document(engine, doc_id)
        if not doc:
            return "Document not found", False
            
        content = doc["content"]
        
        # If document is small enough, return full content
        if len(content) <= MAX_PREVIEW_SIZE:
            return content, False
        
        # For large documents, get preview
        preview = get_document_preview(engine, doc_id, MAX_PREVIEW_SIZE)
        return preview + "\n\n[... Document truncated for display. Full document is saved in database ...]", True
        
    except Exception as e:
        return f"Error loading document: {str(e)}", False

#
# --- UI ---
#
app_ui = ui.page_fluid(
    ui.h3("Mini QDA — Phase 0 (Improved)"),
    ui.layout_sidebar(
        ui.sidebar(
            ui.input_file("file", "Upload a .txt file", accept=[".txt"]),
            ui.tags.div({"id": "file_info", "style": "font-size: 0.8em; color: #666; margin-bottom: 1rem;"}),
            ui.input_text("new_code", "New code name"),
            ui.input_action_button("add_code", "Add code", class_="btn-primary"),
            ui.input_select("code", "Apply code", choices=[]),
            ui.input_action_button("apply", "Apply to selection", class_="btn-success"),
            ui.tags.hr(),
            ui.input_text("open_filename", "Reload by filename (exact)"),
            ui.input_action_button("open", "Open document", class_="btn-secondary"),
            ui.tags.div({"id": "status", "style": "margin-top: 1rem; padding: 0.5rem; display: none;"})
        ),
        ui.tags.div({"id": "docview", "style": "white-space:pre-wrap; border:1px solid #ddd; padding:1rem; min-height:300px; max-height:600px; overflow-y:auto;"}),
        ui.tags.div({"id": "selinfo", "style": "margin-top:0.5rem; color:#666;"}),
        ui.tags.div({"id": "docinfo", "style": "margin-top:0.5rem; font-size: 0.8em; color: #888;"})
    ),
    # Enhanced JS: better selection handling and user feedback
    ui.tags.script("""
      (function(){
        const box = document.getElementById('docview');
        const statusDiv = document.getElementById('status');
        
        function showStatus(message, type = 'info') {
          statusDiv.innerText = message;
          statusDiv.className = type === 'error' ? 'alert alert-danger' : 'alert alert-info';
          statusDiv.style.display = 'block';
          setTimeout(() => {
            statusDiv.style.display = 'none';
          }, 3000);
        }
        
        function getSelectionIn(el) {
          const sel = window.getSelection();
          if (!sel || sel.isCollapsed) return null;
          
          // Check if selection is within our element
          if (!el.contains(sel.anchorNode) || !el.contains(sel.focusNode)) return null;
          
          const range = sel.getRangeAt(0);
          const preSelectionRange = range.cloneRange();
          preSelectionRange.selectNodeContents(el);
          preSelectionRange.setEnd(range.startContainer, range.startOffset);
          
          const selected = sel.toString().trim();
          if (!selected) return null;
          
          // Calculate actual text offset (not HTML offset)
          const textBefore = preSelectionRange.toString();
          const start = textBefore.length;
          const end = start + selected.length;
          
          return { start: start, end: end, text: selected };
        }
        
        document.addEventListener('mouseup', () => {
          const payload = getSelectionIn(box);
          if (payload && payload.text.length > 0) {
            Shiny.setInputValue('selection', payload, {priority: 'event'});
          }
        });
        
        // Expose showStatus for use from Python
        window.showStatus = showStatus;
      })();
    """)
)

#
# --- Server ---
#
def server(input, output, session: shiny_session.Session):
    current_doc_id = reactive.Value(None)
    current_text = reactive.Value("")
    current_metadata = reactive.Value({})

    def show_status(message: str, type_: str = "info"):
        """Show status message to user"""
        escaped_msg = html.escape(str(message))
        session.run_js(f"window.showStatus('{escaped_msg}', '{type_}');")

    def refresh_codes():
        try:
            opts = [{"label": c["name"], "value": str(c["id"])} for c in list_codes(engine)]
            session.send_input_message("code", {"options": opts})
        except Exception as e:
            show_status(f"Error loading codes: {str(e)}", "error")

    @reactive.effect
    def _init():
        refresh_codes()

    @reactive.effect
    @reactive.event(input.add_code)
    def _add_code():
        try:
            name = (input.new_code() or "").strip()
            if not name:
                show_status("Please enter a code name", "error")
                return
            if len(name) > 190:  # Database constraint
                show_status("Code name too long (max 190 characters)", "error")
                return
                
            create_code(engine, name)
            refresh_codes()
            session.send_input_message("new_code", {"value": ""})
            show_status(f"Code '{name}' created successfully")
        except Exception as e:
            show_status(f"Error creating code: {str(e)}", "error")

    @reactive.effect
    @reactive.event(input.file)
    def _on_upload():
        try:
            f = input.file()
            if not f:
                return
            
            file_path = Path(f[0]["datapath"])
            filename = secure_filename(f[0]["name"])
            
            # Show loading message
            session.run_js("document.getElementById('docview').innerHTML = '<em>Processing file...</em>';")
            
            # Read and process file
            text, metadata = sniff_text(file_path, filename)
            
            # Update file info display
            info_html = f"""
            <strong>File:</strong> {html.escape(filename)}<br>
            <strong>Size:</strong> {metadata['size_bytes']:,} bytes<br>
            <strong>Encoding:</strong> {metadata['encoding']}<br>
            <strong>Lines:</strong> {metadata['line_count']:,}<br>
            <strong>Characters:</strong> {metadata['char_count']:,}
            """
            session.run_js(f"document.getElementById('file_info').innerHTML = {info_html!r};")
            
            # Save to database
            doc_id = upsert_document(engine, filename, text)
            current_doc_id.set(doc_id)
            current_text.set(text)
            current_metadata.set(metadata)
            
            _render()
            show_status(f"File '{filename}' uploaded successfully")
            
        except Exception as e:
            show_status(f"Upload failed: {str(e)}", "error")
            session.run_js("document.getElementById('docview').innerHTML = '<em>Upload failed</em>';")

    @reactive.effect
    @reactive.event(input.open)
    def _open_existing():
        try:
            name = (input.open_filename() or "").strip()
            if not name:
                show_status("Please enter a filename", "error")
                return
                
            doc = find_document_by_name(engine, name)
            if doc:
                current_doc_id.set(doc["id"])
                
                # Get text for display (may be truncated for large files)
                display_text, is_preview = get_text_for_display(doc["id"])
                current_text.set(display_text)
                
                # Set metadata
                metadata = {
                    'char_count': len(doc["content"]),
                    'is_preview': is_preview,
                    'filename': doc["filename"]
                }
                current_metadata.set(metadata)
                
                _render()
                
                if is_preview:
                    show_status("Large document - showing preview only")
                else:
                    show_status(f"Document '{name}' loaded successfully")
            else:
                show_status(f"Document '{name}' not found", "error")
                session.run_js(f"document.getElementById('docview').innerText = 'Not found: {html.escape(name)}';")
                
        except Exception as e:
            show_status(f"Error opening document: {str(e)}", "error")

    @reactive.effect
    @reactive.event(input.apply)
    def _apply_code():
        try:
            sel = input.selection()
            code_id = input.code()
            doc_id = current_doc_id.get()
            
            if not sel:
                show_status("Please select text first", "error")
                return
            if not code_id:
                show_status("Please select a code", "error")
                return
            if not doc_id:
                show_status("No document loaded", "error")
                return
                
            # Validate selection bounds
            metadata = current_metadata.get()
            if metadata.get('is_preview'):
                show_status("Cannot code segments in preview mode. Please use smaller documents.", "error")
                return
                
            start, end = int(sel["start"]), int(sel["end"])
            if start < 0 or end < start:
                show_status("Invalid selection bounds", "error")
                return
                
            insert_segment(engine, int(doc_id), int(code_id), start, end, sel["text"])
            _render()  # re-highlight
            show_status(f"Code applied to selected text ({len(sel['text'])} chars)")
            
        except Exception as e:
            show_status(f"Error applying code: {str(e)}", "error")

    def _render():
        try:
            doc_id = current_doc_id.get()
            text = current_text.get()
            metadata = current_metadata.get()
            
            if not doc_id or not text:
                return
                
            # Get segments and build HTML
            segments = list_segments(engine, int(doc_id))
            html_content = highlight_text(text, segments)
            session.run_js(f"document.getElementById('docview').innerHTML = {html_content!r};")
            
            # Update document info
            info_parts = []
            if metadata.get('char_count'):
                info_parts.append(f"Document: {metadata['char_count']:,} characters")
            if len(segments) > 0:
                info_parts.append(f"{len(segments)} coded segment(s)")
            if metadata.get('is_preview'):
                info_parts.append("(Preview mode - full document in database)")
                
            info_text = " | ".join(info_parts)
            session.run_js(f"document.getElementById('docinfo').innerText = {info_text!r};")
            
            # Show selection info
            sel = input.selection()
            if sel:
                sel_info = f"Selected: {sel['start']}–{sel['end']} ({len(sel['text'])} chars)"
                session.run_js(f"document.getElementById('selinfo').innerText = {sel_info!r};")
            else:
                session.run_js("document.getElementById('selinfo').innerText = '';")
                
        except Exception as e:
            show_status(f"Error rendering document: {str(e)}", "error")

app = App(app_ui, server)

if __name__ == "__main__":
    from shiny import run_app
    run_app(app, host="127.0.0.1", port=8000)
