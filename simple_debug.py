from __future__ import annotations

import os
import traceback
from datetime import datetime

from shiny import App, ui, reactive, render

# Simple debug UI - no database dependencies initially
debug_ui = ui.page_fluid(
    ui.h2("Mini QDA Debug Console"),
    ui.input_action_button("test_env", "Test Environment", class_="btn-primary"),
    ui.input_action_button("test_imports", "Test Imports", class_="btn-secondary"), 
    ui.input_action_button("test_db", "Test Database", class_="btn-warning"),
    ui.tags.hr(),
    ui.output_ui("results")
)

def debug_server(input, output, session):
    test_results = reactive.Value([])
    
    def add_result(message: str, success: bool = True):
        """Add test result"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        icon = "✓" if success else "✗"
        style = "color: green;" if success else "color: red;"
        test_results.set(test_results.get() + [
            ui.div(f"[{timestamp}] {icon} {message}", style=style)
        ])
    
    @output
    @render.ui
    def results():
        """Display test results"""
        results = test_results.get()
        if not results:
            return ui.div("Click buttons above to run tests.")
        return ui.div(*results, style="font-family: monospace; padding: 1rem; border: 1px solid #ddd;")
    
    @reactive.effect
    @reactive.event(input.test_env)
    def _test_environment():
        test_results.set([])  # Clear previous results
        add_result("Testing environment variables...")
        
        env_vars = ["DB_HOST", "DB_USER", "DB_PASS", "DB_NAME", "DB_PORT"]
        all_good = True
        
        for var in env_vars:
            value = os.getenv(var)
            if value:
                # Mask password
                display_value = "***" if var == "DB_PASS" else value
                add_result(f"{var}: {display_value}", True)
            else:
                add_result(f"{var}: NOT SET", False)
                all_good = False
        
        if all_good:
            add_result("All environment variables are set!", True)
        else:
            add_result("Some environment variables are missing!", False)
    
    @reactive.effect
    @reactive.event(input.test_imports)
    def _test_imports():
        test_results.set([])  # Clear previous results
        add_result("Testing package imports...")
        
        packages = [
            ("shiny", "Shiny for Python web framework"),
            ("sqlalchemy", "Database ORM"),
            ("pymysql", "MySQL driver"),
            ("chardet", "Character encoding detection"),
            ("cryptography", "SSL/TLS support")
        ]
        
        import_success = True
        for pkg, description in packages:
            try:
                __import__(pkg)
                add_result(f"{pkg}: Available ({description})", True)
            except ImportError as e:
                add_result(f"{pkg}: MISSING - {str(e)}", False)
                import_success = False
        
        if import_success:
            add_result("All required packages are available!", True)
        else:
            add_result("Some packages are missing - check requirements.txt", False)
    
    @reactive.effect
    @reactive.event(input.test_db)
    def _test_database():
        test_results.set([])  # Clear previous results
        add_result("Testing database connection...")
        
        try:
            # Check environment first
            required_vars = ["DB_HOST", "DB_USER", "DB_PASS", "DB_NAME"]
            missing_vars = [var for var in required_vars if not os.getenv(var)]
            
            if missing_vars:
                add_result(f"Missing environment variables: {missing_vars}", False)
                return
            
            add_result("Environment variables OK", True)
            
            # Test imports
            try:
                from sqlalchemy import create_engine, text
                add_result("SQLAlchemy import: OK", True)
            except ImportError as e:
                add_result(f"SQLAlchemy import failed: {e}", False)
                return
            
            # Test basic connection
            host = os.getenv("DB_HOST")
            port = os.getenv("DB_PORT", "3306")
            user = os.getenv("DB_USER")
            pwd = os.getenv("DB_PASS")
            db = os.getenv("DB_NAME")
            
            add_result(f"Attempting connection to {host}:{port}", True)
            
            # Try different connection methods
            connection_attempts = [
                ("Basic connection", f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"),
                ("No SSL", f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?ssl_disabled=true"),
                ("Charset specified", f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset=utf8mb4")
            ]
            
            connected = False
            for attempt_name, url in connection_attempts:
                try:
                    add_result(f"Trying {attempt_name}...", True)
                    engine = create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 10})
                    
                    with engine.connect() as conn:
                        result = conn.execute(text("SELECT 1 as test, VERSION() as version")).fetchone()
                        add_result(f"{attempt_name}: SUCCESS! MySQL version: {result.version}", True)
                        connected = True
                        break
                        
                except Exception as e:
                    add_result(f"{attempt_name}: FAILED - {str(e)[:100]}", False)
            
            if not connected:
                add_result("All connection attempts failed!", False)
                
                # Test basic network connectivity
                try:
                    import socket
                    add_result("Testing network connectivity...", True)
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result = sock.connect_ex((host, int(port)))
                    sock.close()
                    
                    if result == 0:
                        add_result("Network connectivity: OK (can reach host:port)", True)
                        add_result("Network OK but database auth/config issue", False)
                    else:
                        add_result(f"Network connectivity: FAILED (code: {result})", False)
                        add_result("Cannot reach the database server", False)
                        
                except Exception as e:
                    add_result(f"Network test error: {str(e)}", False)
            
        except Exception as e:
            add_result(f"Unexpected error: {str(e)}", False)
            add_result(f"Traceback: {traceback.format_exc()[:500]}", False)

# Create debug app
debug_app = App(debug_ui, debug_server)

if __name__ == "__main__":
    from shiny import run_app
    run_app(debug_app, host="127.0.0.1", port=8000)
