import os
import fnmatch
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import pyperclip  # For clipboard functionality

# Install required module: pip install pyperclip

# Define folders and files to ignore
IGNORE_FOLDERS = {
    '__pycache__',
    'migrations',
    'venv',
    'env',
    'node_modules',
    '.git'
}

IGNORE_FILE_PATTERNS = {
    '*.pyc',
    '*.pyo',
    '*.pyd',
    '.DS_Store',
    'thumbs.db',
    '.gitignore',
    '.env',
    '*.log'
}

class FileTreeApp:
    def __init__(self, root):
        self.root = root
        self.root.title("File Content Extractor")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 600)

        # Variables
        self.current_dir = os.getcwd()
        self.all_files = []
        self.filtered_files = []
        self.checkbox_vars = {}  # Persistent: file path -> BooleanVar

        # Configure styles
        self.setup_styles()

        # Setup UI
        self.setup_ui()

        # Load files
        self.load_files()

    def setup_styles(self):
        style = ttk.Style()
        style.configure("Treeview", rowheight=25)
        style.configure("Treeview.Heading", font=('Arial', 10, 'bold'))

    def setup_ui(self):
        # Main container
        main_container = ttk.Frame(self.root)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Top frame for controls
        top_frame = ttk.Frame(main_container)
        top_frame.pack(fill=tk.X, pady=(0, 10))

        # Directory selection
        ttk.Label(top_frame, text="Directory:").pack(side=tk.LEFT, padx=(0, 5))
        self.dir_var = tk.StringVar(value=self.current_dir)
        dir_entry = ttk.Entry(top_frame, textvariable=self.dir_var, width=50)
        dir_entry.pack(side=tk.LEFT, padx=(0, 5))

        ttk.Button(top_frame, text="Browse", command=self.browse_directory).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(top_frame, text="Refresh", command=self.load_files).pack(side=tk.LEFT, padx=(0, 10))

        # Search frame
        search_frame = ttk.Frame(main_container)
        search_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(search_frame, text="Search:").pack(side=tk.LEFT, padx=(0, 5))
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=self.search_var, width=40)
        search_entry.pack(side=tk.LEFT, padx=(0, 5))
        search_entry.bind('<KeyRelease>', self.filter_files)

        ttk.Button(search_frame, text="Clear", command=self.clear_search).pack(side=tk.LEFT, padx=(0, 10))

        # Treeview frame with scrollbars
        tree_frame = ttk.Frame(main_container)
        tree_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # Create treeview
        self.tree = ttk.Treeview(tree_frame, columns=("Select", "File"), show="tree headings")
        self.tree.heading("#0", text="")
        self.tree.heading("Select", text="Select")
        self.tree.heading("File", text="File")

        # Configure columns
        self.tree.column("#0", width=0, stretch=False)
        self.tree.column("Select", width=60, anchor="center")
        self.tree.column("File", width=400)

        # Add scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        # Grid layout
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        # Selection buttons frame
        selection_frame = ttk.Frame(main_container)
        selection_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Button(selection_frame, text="Check All", command=self.check_all).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(selection_frame, text="Uncheck All", command=self.uncheck_all).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(selection_frame, text="Check *.py", command=lambda: self.check_pattern("*.py")).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(selection_frame, text="Check *.txt", command=lambda: self.check_pattern("*.txt")).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(selection_frame, text="Check *.md", command=lambda: self.check_pattern("*.md")).pack(side=tk.LEFT)

        # Bottom frame for actions and output
        bottom_frame = ttk.Frame(main_container)
        bottom_frame.pack(fill=tk.BOTH, expand=True)

        # Left frame for action buttons
        action_frame = ttk.Frame(bottom_frame)
        action_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))

        # Output filename
        ttk.Label(action_frame, text="Output File:").pack(anchor=tk.W, pady=(0, 5))
        self.output_var = tk.StringVar(value="files_content.txt")
        output_entry = ttk.Entry(action_frame, textvariable=self.output_var, width=30)
        output_entry.pack(fill=tk.X, pady=(0, 10))

        # Action buttons
        ttk.Button(action_frame, text="Generate Output", command=self.generate_output).pack(fill=tk.X, pady=5)
        ttk.Button(action_frame, text="Preview Selected", command=self.preview_selected).pack(fill=tk.X, pady=5)
        ttk.Button(action_frame, text="Copy to Clipboard", command=self.copy_to_clipboard).pack(fill=tk.X, pady=5)
        ttk.Button(action_frame, text="Save to File", command=self.save_to_file).pack(fill=tk.X, pady=5)

        # Status label
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(action_frame, textvariable=self.status_var).pack(anchor=tk.W, pady=(10, 0))

        # Right frame for output display
        output_frame = ttk.LabelFrame(bottom_frame, text="Output Preview")
        output_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        # Scrolled text for output
        self.output_text = scrolledtext.ScrolledText(output_frame, wrap=tk.WORD, width=60, height=20)
        self.output_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Configure tags for syntax highlighting
        self.output_text.tag_configure("header", foreground="blue", font=('Arial', 10, 'bold'))
        self.output_text.tag_configure("path", foreground="darkgreen")
        self.output_text.tag_configure("separator", foreground="gray")

    def should_ignore(self, path, is_dir=False):
        """Check if a path should be ignored based on ignore rules"""
        # Check folder ignore
        if is_dir:
            dir_name = os.path.basename(path)
            if dir_name in IGNORE_FOLDERS:
                return True
            # Also check if any part of the path contains ignored folders
            for folder in IGNORE_FOLDERS:
                if folder in path.split(os.sep):
                    return True
            return False

        # Check file ignore patterns
        filename = os.path.basename(path)
        for pattern in IGNORE_FILE_PATTERNS:
            if fnmatch.fnmatch(filename, pattern):
                return True

        # Check if file is in ignored folder
        for folder in IGNORE_FOLDERS:
            if folder in path.split(os.sep):
                return True

        return False

    def get_all_files(self, base_dir):
        """Get all files in directory tree with relative paths, ignoring specified folders"""
        all_files = []

        for root, dirs, files in os.walk(base_dir, topdown=True):
            # Remove ignored directories from dirs list so os.walk doesn't traverse them
            dirs[:] = [d for d in dirs if not self.should_ignore(os.path.join(root, d), is_dir=True)]

            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, base_dir)

                # Skip ignored files
                if not self.should_ignore(rel_path, is_dir=False):
                    all_files.append(rel_path)

        return sorted(all_files)

    def load_files(self):
        """Load files from current directory"""
        self.current_dir = self.dir_var.get()
        if not os.path.exists(self.current_dir):
            messagebox.showerror("Error", f"Directory does not exist: {self.current_dir}")
            return

        try:
            self.all_files = self.get_all_files(self.current_dir)
            # Create persistent BooleanVar for each file (all unchecked initially)
            self.checkbox_vars = {file: tk.BooleanVar(value=False) for file in self.all_files}
            self.filtered_files = self.all_files.copy()
            self.update_treeview()
            self.status_var.set(f"Loaded {len(self.all_files)} files from {self.current_dir}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load files: {str(e)}")

    def update_treeview(self):
        """Update the treeview with current filtered files, preserving checkbox states"""
        # Clear treeview
        for item in self.tree.get_children():
            self.tree.delete(item)

        # Add files from filtered_files using existing BooleanVars
        for file_path in self.filtered_files:
            var = self.checkbox_vars[file_path]  # should always exist
            display_symbol = "✓" if var.get() else "□"
            self.tree.insert("", "end", values=(display_symbol, file_path))

        # Bind click event (rebind each time to ensure it's active)
        self.tree.bind('<Button-1>', self.on_tree_click)

    def on_tree_click(self, event):
        """Handle clicks on the treeview to toggle checkboxes"""
        region = self.tree.identify_region(event.x, event.y)
        if region == "cell":
            column = self.tree.identify_column(event.x)
            if column == "#1":  # First column (Select)
                item = self.tree.identify_row(event.y)
                if item:
                    file_path = self.tree.item(item)['values'][1]
                    if file_path in self.checkbox_vars:
                        current = self.checkbox_vars[file_path].get()
                        self.checkbox_vars[file_path].set(not current)
                        self.update_checkbox_display(item, not current)

    def update_checkbox_display(self, item, checked):
        """Update the checkbox display in treeview for a given item"""
        symbol = "✓" if checked else "□"
        self.tree.set(item, "Select", symbol)

    def filter_files(self, event=None):
        """Filter files based on search term, preserving checkbox states"""
        search_term = self.search_var.get().lower()
        if not search_term:
            self.filtered_files = self.all_files.copy()
        else:
            self.filtered_files = [
                f for f in self.all_files
                if search_term in f.lower() or search_term in os.path.basename(f).lower()
            ]

        self.update_treeview()
        self.status_var.set(f"Showing {len(self.filtered_files)} of {len(self.all_files)} files")

    def clear_search(self):
        """Clear search filter"""
        self.search_var.set("")
        self.filter_files()

    def browse_directory(self):
        """Open directory browser dialog"""
        directory = filedialog.askdirectory(initialdir=self.current_dir)
        if directory:
            self.dir_var.set(directory)
            self.load_files()

    def check_all(self):
        """Check all visible files"""
        for item in self.tree.get_children():
            file_path = self.tree.item(item)['values'][1]
            if file_path in self.checkbox_vars:
                self.checkbox_vars[file_path].set(True)
                self.update_checkbox_display(item, True)

    def uncheck_all(self):
        """Uncheck all visible files"""
        for item in self.tree.get_children():
            file_path = self.tree.item(item)['values'][1]
            if file_path in self.checkbox_vars:
                self.checkbox_vars[file_path].set(False)
                self.update_checkbox_display(item, False)

    def check_pattern(self, pattern):
        """Check files matching pattern among visible files"""
        for item in self.tree.get_children():
            file_path = self.tree.item(item)['values'][1]
            if fnmatch.fnmatch(file_path, pattern) or fnmatch.fnmatch(os.path.basename(file_path), pattern):
                if file_path in self.checkbox_vars:
                    self.checkbox_vars[file_path].set(True)
                    self.update_checkbox_display(item, True)

    def get_selected_files(self):
        """Get list of selected files (checked)"""
        selected = []
        for file_path, var in self.checkbox_vars.items():
            if var.get():
                selected.append(file_path)
        return sorted(selected)

    def read_file_content(self, file_path):
        """Read content of a file with error handling"""
        full_path = os.path.join(self.current_dir, file_path)
        try:
            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        except PermissionError:
            return f"[PERMISSION DENIED: Cannot read {file_path}]"
        except UnicodeDecodeError:
            return f"[BINARY FILE or ENCODING ISSUE: Cannot read as text {file_path}]"
        except Exception as e:
            return f"[ERROR reading {file_path}: {str(e)}]"

    def generate_output(self):
        """Generate output from selected files and display in text widget"""
        selected_files = self.get_selected_files()

        if not selected_files:
            messagebox.showwarning("No Selection", "Please select at least one file.")
            return

        # Clear output text
        self.output_text.delete(1.0, tk.END)

        # Generate output
        output = "FILES AND THEIR CONTENT\n"
        output += "=" * 80 + "\n\n"

        for idx, rel_path in enumerate(selected_files, 1):
            # Add file header
            self.output_text.insert(tk.END, f"FILE {idx}: ", "header")
            self.output_text.insert(tk.END, f"{rel_path}\n", "path")
            self.output_text.insert(tk.END, "-" * 80 + "\n", "separator")

            # Add file content
            content = self.read_file_content(rel_path)
            self.output_text.insert(tk.END, content + "\n")

            # Add separator between files (unless it's the last file)
            if idx < len(selected_files):
                self.output_text.insert(tk.END, "=" * 80 + "\n\n", "separator")

        self.status_var.set(f"Generated output for {len(selected_files)} files")

        # Auto-scroll to top
        self.output_text.see(1.0)

    def preview_selected(self):
        """Preview selected files in a separate window"""
        selected_files = self.get_selected_files()

        if not selected_files:
            messagebox.showwarning("No Selection", "Please select at least one file.")
            return

        # Create preview window
        preview_window = tk.Toplevel(self.root)
        preview_window.title(f"Preview ({len(selected_files)} files)")
        preview_window.geometry("900x600")

        # Create notebook for tabs
        notebook = ttk.Notebook(preview_window)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Create a tab for each file
        for file_path in selected_files:
            frame = ttk.Frame(notebook)
            notebook.add(frame, text=os.path.basename(file_path)[:20] + "...")

            # Add text widget with scrollbar
            text_widget = scrolledtext.ScrolledText(frame, wrap=tk.WORD)
            text_widget.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

            # Load content
            content = self.read_file_content(file_path)
            text_widget.insert(1.0, content)
            text_widget.config(state=tk.DISABLED)  # Make read-only

        # Add close button
        ttk.Button(preview_window, text="Close", command=preview_window.destroy).pack(pady=10)

    def copy_to_clipboard(self):
        """Copy output text to clipboard"""
        content = self.output_text.get(1.0, tk.END)
        if content.strip():
            pyperclip.copy(content)
            self.status_var.set("Content copied to clipboard!")
            messagebox.showinfo("Success", "Content copied to clipboard!")
        else:
            messagebox.showwarning("Empty", "No content to copy. Generate output first.")

    def save_to_file(self):
        """Save output to file"""
        content = self.output_text.get(1.0, tk.END)
        if not content.strip():
            messagebox.showwarning("Empty", "No content to save. Generate output first.")
            return

        output_file = self.output_var.get()
        if not output_file:
            output_file = "files_content.txt"

        # Ask for save location
        file_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            initialfile=output_file,
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )

        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                self.status_var.set(f"Saved to: {file_path}")
                messagebox.showinfo("Success", f"File saved successfully:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save file: {str(e)}")

def main():
    root = tk.Tk()
    app = FileTreeApp(root)
    root.mainloop()

if __name__ == "__main__":
    # Install required module: pip install pyperclip
    try:
        import pyperclip
    except ImportError:
        print("Installing required module: pyperclip")
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyperclip"])
        import pyperclip

    main()
