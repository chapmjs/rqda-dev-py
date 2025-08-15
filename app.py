from __future__ import annotations

import os, html, chardet
from pathlib import Path
from typing import List, Dict

from shiny import App, ui, reactive, render
from sqlalchemy import text
from db import get_engine, init_db, upsert_document, get_document, find_document_by_name, list_codes, create_code, insert_segment, list_segments

# Configuration constants
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB limit

def secure_filename(name: str) -> str:
    import re
    name = name.replace("\\", "/").split("/")[-1]
    name = re.sub(r'[^A-Za-z0-9._-]+', '_', name)
    if name in ('', '.', '..'):
        name = 'upload.txt'
    return name[:200]

def detect_encoding_and_read(file_path: Path) -> str:
    """Detect file encoding and read content safely."""
    # Read a sample for encoding detection
    with open(file_path, 'rb') as f:
        raw_sample = f.read(10000)
    
    # Detect encoding
    detection_result = chardet.detect(raw_sample)
    detected_encoding = detection_result.get('encoding') or 'utf-8'
    
    # Try to read with detected encoding, fall back to utf-8 with replacement
    try:
        content = file_path.read_text(encoding=detected_encoding)
    except (UnicodeDecodeError, LookupError):
        content = file_path.read_text(encoding='utf-8', errors='replace')
    
    return content

def validate_file_size(file_path: Path) -> None:
    """Validate file size before processing."""
    size = file_path.stat().st_size
    if size > MAX_FILE_SIZE:
        raise ValueError(f"File too large: {size:,} bytes (max: {MAX_FILE_SIZE:,} bytes)")

def sniff_text(file_path: Path, filename: str) -> str:
    """Read and validate text file."""
    validate_file_size(file_path)
    return detect_encoding_and_read(file_path)

def highlight_text(text: str, segments: List[Dict]) -> str:
    """Return HTML with <mark> wrapped around coded segments."""
    if not segments:
        return html.escape(text)

    # Sort segments and handle overlaps
    sorted_segments = sorted(segments, key=lambda s: (s["start_offset"], s["end_offset"]))
    
    parts = []
    last = 0
    
    for seg in sorted_segments:
        s = max(0, min(seg["start_offset"], len(text)))
        e = max(s, min(seg["end_offset"], len(text)))
        
        # Skip overlapping segments
        if s < last:
            continue
            
        # Add text before segment
        if s > last:
            parts.append(html.escape(text[last:s]))
        
        # Add highlighted segment
        parts.append("<mark>" + html.escape(text[s:e]) + "</mark>")
        last = e
    
    # Add remaining text
    if last < len(text):
        parts.append(html.escape(text[last:]))
    
    return "".join(parts)

#
# --- Setup DB engine and ensure schema exists at startup ---
#
engine = get_engine()
init_db(engine)

#
# --- UI ---
#
app_ui = ui.page_fluid(
    ui.h3("Mini QDA — Simplified"),
    ui.layout_sidebar(
        ui.sidebar(
            ui.input_file("file", "Upload a .txt file", accept=[".txt"]),
            ui.output_text("file_status"),
            ui.tags.hr(),
            ui.input_text("new_code", "New code name"),
            ui.input_action_button("add_code", "Add code", class_="btn-primary"),
            ui.input_action_button("refresh_codes", "Refresh Codes", class_="btn-info"),
            ui.output_text("code_status"),
            ui.tags.hr(),
            ui.output_ui("code_select"),
            ui.input_action_button("apply", "Apply to selection", class_="btn-success"),
            ui.output_text("apply_status"),
            ui.tags.hr(),
            ui.input_text("open_filename", "Open document by filename"),
            ui.input_action_button("open", "Open document", class_="btn-secondary"),
            ui.output_text("open_status"),
        ),
        ui.output_ui("document_display"),
        ui.output_text("selection_info"),
        ui.output_text("document_info")
    )
)

#
# --- Server ---
#
def server(input, output, session):
    # Reactive values
    current_doc_id = reactive.Value(None)
    current_text = reactive.Value("")
    current_selection = reactive.Value(None)
    codes_list = reactive.Value([])  # Add reactive codes list
    code_status_message = reactive.Value("")

    def refresh_codes():
        """Refresh the codes list"""
        try:
            codes = list_codes(engine)
            print(f"DEBUG: Fetched {len(codes)} codes from database")  # Debug
            for code in codes:
                print(f"  - {code.get('name', 'Unknown')} (ID: {code.get('id', 'Unknown')})")  # Debug
            codes_list.set(codes)  # Update reactive value
            return codes
        except Exception as e:
            print(f"Error loading codes: {str(e)}")
            codes_list.set([])  # Set empty list on error
            return []

    @output
    @render.ui
    def code_select():
        """Render the code selection dropdown reactively"""
        codes = codes_list.get()
        print(f"DEBUG: Rendering dropdown with {len(codes)} codes")  # Debug
        
        if not codes:
            choices = [{"label": "No codes available", "value": ""}]
        else:
            choices = [{"label": c["name"], "value": str(c["id"])} for c in codes]
            print(f"DEBUG: Choices = {choices}")  # Debug
            
        return ui.input_select("code", "Apply code", choices=choices)

    @reactive.effect
    def _update_code_choices():
        """Update code dropdown when codes list changes"""
        # This is no longer needed since we're using render.ui
        pass

    @reactive.effect
    def _init():
        """Initialize the app"""
        refresh_codes()

    @output
    @render.text
    def file_status():
        """Show file upload status"""
        return ""

    @output
    @render.text
    def code_status():
        """Show code creation status"""
        return code_status_message.get()

    @output
    @render.text
    def apply_status():
        """Show code application status"""
        return ""

    @output
    @render.text
    def open_status():
        """Show document open status"""
        return ""

    @output
    @render.text
    def selection_info():
        """Show current selection info"""
        sel = current_selection.get()
        if sel:
            return f"Selected: {sel['start']}-{sel['end']} ({len(sel['text'])} chars)"
        return "No text selected"

    @output
    @render.text
    def document_info():
        """Show document info"""
        doc_id = current_doc_id.get()
        text = current_text.get()
        if doc_id and text:
            try:
                segments = list_segments(engine, int(doc_id))
                return f"Document: {len(text):,} chars | {len(segments)} coded segments"
            except:
                return f"Document: {len(text):,} chars"
        return "No document loaded"

    @output
    @render.ui
    def document_display():
        """Display the document with highlighting"""
        doc_id = current_doc_id.get()
        text = current_text.get()
        
        if not doc_id or not text:
            return ui.div("No document loaded", style="padding: 1rem; border: 1px solid #ddd; min-height: 300px;")
        
        try:
            segments = list_segments(engine, int(doc_id))
            html_content = highlight_text(text, segments)
            
            return ui.div(
                ui.HTML(html_content),
                id="docview",
                style="white-space: pre-wrap; padding: 1rem; border: 1px solid #ddd; min-height: 300px; max-height: 600px; overflow-y: auto; user-select: text;",
                # Simple click handler to capture selections
                onclick="captureSelection()"
            )
        except Exception as e:
            return ui.div(f"Error: {str(e)}", style="padding: 1rem; border: 1px solid #ddd; color: red;")

    @reactive.effect
    @reactive.event(input.add_code)
    def _add_code():
        """Add a new code"""
        name = (input.new_code() or "").strip()
        
        if not name:
            code_status_message.set("Please enter a code name")
            return
        
        code_status_message.set(f"Creating code '{name}'...")
        
        try:
            # Test database connection first
            code_status_message.set("Testing database connection...")
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).fetchone()
                code_status_message.set("Database connection OK")
            
            # Try to create the code
            code_status_message.set(f"Adding code '{name}' to database...")
            code_id = create_code(engine, name)
            code_status_message.set(f"Code '{name}' created with ID {code_id}")
            
            # Refresh the codes list
            code_status_message.set("Refreshing codes list...")
            codes = refresh_codes()
            code_status_message.set(f"Found {len(codes)} total codes. Dropdown should update now.")
            
            # Clear the input
            ui.update_text("new_code", value="")
            code_status_message.set(f"Success! Code '{name}' added. Check dropdown above.")
            
        except Exception as e:
            error_msg = f"Error creating code: {str(e)}"
            print(error_msg)  # Debug output to logs
            code_status_message.set(error_msg)
            
            # Also try to get more specific error info
            import traceback
            print("Full traceback:")
            print(traceback.format_exc())

    @reactive.effect
    @reactive.event(input.refresh_codes)
    def _manual_refresh():
        """Manual refresh of codes for debugging"""
        code_status_message.set("Manually refreshing codes...")
        try:
            codes = refresh_codes()
            code_status_message.set(f"Manual refresh: Found {len(codes)} codes")
        except Exception as e:
            code_status_message.set(f"Manual refresh failed: {e}")

    @reactive.effect
    @reactive.event(input.file)
    def _on_upload():
        """Handle file upload"""
        f = input.file()
        if not f:
            return
        
        try:
            file_path = Path(f[0]["datapath"])
            filename = secure_filename(f[0]["name"])
            
            # Read file
            text = sniff_text(file_path, filename)
            
            # Save to database
            doc_id = upsert_document(engine, filename, text)
            current_doc_id.set(doc_id)
            current_text.set(text)
            
        except Exception as e:
            pass  # Error handling simplified

    @reactive.effect
    @reactive.event(input.open)
    def _open_existing():
        """Open existing document"""
        name = (input.open_filename() or "").strip()
        if not name:
            return
        
        try:
            doc = find_document_by_name(engine, name)
            if doc:
                current_doc_id.set(doc["id"])
                current_text.set(doc["content"])
        except Exception as e:
            pass  # Error handling simplified

    @reactive.effect
    @reactive.event(input.apply)
    def _apply_code():
        """Apply code to selection"""
        # For now, we'll use a simple approach without complex selection detection
        # This can be enhanced later once the basic app is working
        pass

# Add some basic JavaScript for selection detection
app_ui = ui.page_fluid(
    ui.h3("Mini QDA — Simplified"),
    ui.layout_sidebar(
        ui.sidebar(
            ui.input_file("file", "Upload a .txt file", accept=[".txt"]),
            ui.output_text("file_status"),
            ui.tags.hr(),
            ui.input_text("new_code", "New code name"),
            ui.input_action_button("add_code", "Add code", class_="btn-primary"),
            ui.output_text("code_status"),
            ui.tags.hr(),
            ui.input_select("code", "Apply code", choices=[]),
            ui.input_action_button("apply", "Apply to selection", class_="btn-success"),
            ui.output_text("apply_status"),
            ui.tags.hr(),
            ui.input_text("open_filename", "Open document by filename"),
            ui.input_action_button("open", "Open document", class_="btn-secondary"),
            ui.output_text("open_status"),
        ),
        ui.output_ui("document_display"),
        ui.output_text("selection_info"),
        ui.output_text("document_info")
    ),
    ui.tags.script("""
        function captureSelection() {
            // Simple selection capture - can be enhanced later
            const selection = window.getSelection();
            if (selection && !selection.isCollapsed) {
                const text = selection.toString();
                if (text.trim()) {
                    // For now, just log the selection
                    console.log('Selected:', text);
                }
            }
        }
    """)
)

app = App(app_ui, server)

if __name__ == "__main__":
    from shiny import run_app
    run_app(app, host="127.0.0.1", port=8000)
