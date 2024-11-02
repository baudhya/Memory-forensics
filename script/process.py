import tkinter as tk
from tkinter import messagebox
from array import array

class AddressManager:
    def __init__(self):
        self.address_dict = {}  # Store current values
        self.left_stack = []    # Store historical changes (previous values)
        self.value_history = {} # Store all values for each address with timestamps
        self.NUM = 2 ** 10

    def parse_binary_file2(self, fp):
        """Read and parse the binary file to extract address and data."""
        data = fp.read(8)

        if len(data) != 8:
            raise ValueError("Invalid data size. Expected 16 bytes.")

        address = int.from_bytes(data, byteorder='little')

        # Use array for efficient storage of 8-bit integers
        data = array('B')
        for _ in range(64):
            d = fp.read(1)
            if not d:
                raise ValueError("Unexpected EOF")
            data.append(d[0])  # Directly append byte value

        return [(address + i, data[i]) for i in range(64)]

    def load_data_from_file(self, file_path):
        """Load the data from a binary file and update address values."""
        with open(file_path, 'rb') as fp:
            try:
                index  = 0
                while True and index <= self.NUM:
                    data_value = self.parse_binary_file2(fp)
                    data_to_push = array('B')  # Store previous and new values
                    address_push = data_value[0][0]

                    # Optimize dictionary lookup and update
                    for address, data in data_value:
                        prev_data = self.address_dict.get(address, 0x0)  # Get previous value
                        data_to_push.append(prev_data)  # Store previous value
                        data_to_push.append(data)       # Store new value

                        # Update value history with timestamp
                        if address not in self.value_history:
                            self.value_history[address] = []
                        timestamp = f"T-{len(self.value_history[address])}"
                        self.value_history[address].append((timestamp, data))

                    # Update the dictionary
                    for i, (address, new_data) in enumerate(data_value):
                        self.address_dict[address] = new_data

                    # Append changes to the left stack
                    self.left_stack.append((address_push, data_to_push))
                    index += 1
            except Exception as e:
                print(f"Error or End of File: {e}")

    def query_address(self, address):
        """Query the current value and get value history of an address."""
        current_value = self.address_dict.get(address, None)
        history = self.value_history.get(address, [])
        if current_value is None:
            return f"Address {address} not found."
        return {
            "address": address,
            "current_value": current_value,
            "history": history
        }

    def print_addresses(self):
        """Return all available addresses as a list."""
        return sorted(self.address_dict.keys())


class AddressManagerGUI:
    def __init__(self, master, address_manager):
        self.master = master
        self.address_manager = address_manager

        # GUI Layout
        master.title("Address Manager")

        # Address Listbox
        self.address_list_label = tk.Label(master, text="Available Addresses:")
        self.address_list_label.pack()

        self.address_listbox = tk.Listbox(master)
        self.address_listbox.pack()
        self.populate_address_listbox()

        # Select Button
        self.select_button = tk.Button(master, text="Select Address", command=self.select_address)
        self.select_button.pack()

        # Display Current Value
        self.result_text = tk.Text(master, height=4, width=50)
        self.result_text.pack()

        # Value History Listbox
        self.value_history_label = tk.Label(master, text="Value History with Timestamps:")
        self.value_history_label.pack()

        self.value_history_listbox = tk.Listbox(master)
        self.value_history_listbox.pack()

    def populate_address_listbox(self):
        """Populate the listbox with available addresses."""
        addresses = self.address_manager.print_addresses()
        for address in addresses:
            self.address_listbox.insert(tk.END, address)

    def select_address(self):
        """Select an address from the listbox, query it, and display its history."""
        try:
            selected_address = int(self.address_listbox.get(self.address_listbox.curselection()))
            result = self.address_manager.query_address(selected_address)
            if isinstance(result, dict):
                # Display current value in result_text
                self.result_text.delete(1.0, tk.END)
                self.result_text.insert(tk.END, f"Address: {result['address']}\n")
                self.result_text.insert(tk.END, f"Current Value: {result['current_value']}\n")
                
                # Populate the value history listbox
                self.value_history_listbox.delete(0, tk.END)  # Clear previous history
                for timestamp, value in result["history"]:
                    self.value_history_listbox.insert(tk.END, f"{timestamp}: {value}")

            else:
                messagebox.showinfo("Info", result)
        except IndexError:
            messagebox.showerror("Error", "Please select an address.")
        except ValueError:
            messagebox.showerror("Error", "Invalid address format.")


# Run the application
file_path = 'data.log'

# Load data and create AddressManager instance
address_manager = AddressManager()
address_manager.load_data_from_file(file_path)

# Set up the GUI
root = tk.Tk()
app = AddressManagerGUI(root, address_manager)
root.mainloop()
