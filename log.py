import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, log_dir, line_count):
        self.log_dir = log_dir
        self.line_count = line_count
        self.last_positions = {}
        self.refresh_logs()

    def on_any_event(self, event):
        self.refresh_logs()

    def refresh_logs(self):
        self.file_paths = self.get_sorted_log_files()
        self.print_new_lines()

    def get_sorted_log_files(self):
        log_files = [f for f in os.listdir(self.log_dir) if f.startswith('info.log')]
        sorted_files = []
        for file in log_files:
            full_path = os.path.join(self.log_dir, file)
            try:
                sorted_files.append((file, os.path.getmtime(full_path)))
            except FileNotFoundError:
                # File was deleted before it could be accessed, skip it
                continue
        return [file for file, _ in sorted(sorted_files, key=lambda x: x[1], reverse=True)]

    def read_file_with_retries(self, file_path, retries=10, delay=1):
        while retries > 0:
            try:
                with open(file_path, 'r') as file:
                    return file.readlines()
            except (FileNotFoundError, PermissionError):
                time.sleep(delay)
                retries -= 1
        return []

    def print_new_lines(self):
        all_new_lines = []
        for file_path in self.file_paths:
            full_path = os.path.join(self.log_dir, file_path)
            lines = self.read_file_with_retries(full_path)
            if not lines:
                print(f"Error accessing file: {full_path}")
                continue

            if file_path in self.last_positions:
                start_pos = self.last_positions[file_path]
                new_lines = lines[start_pos:]
            else:
                new_lines = lines

            all_new_lines.extend(new_lines)
            self.last_positions[file_path] = len(lines)

        for line in all_new_lines[-self.line_count:]:
            print(line, end='')

def main():
    log_dir = 'log'
    line_count = 5000
    event_handler = LogFileHandler(log_dir, line_count)
    observer = Observer()
    observer.schedule(event_handler, path=log_dir, recursive=False)
    observer.start()

    print(f"Monitoring log directory '{log_dir}' for changes...")

    try:
        while True:
            time.sleep(0.25)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()

if __name__ == "__main__":
    main()