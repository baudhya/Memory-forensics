# from array import array

# def parse_binary_file2(fp):
#     # Read 16 bytes (128 bits: 64 bits for address + 64 bits for data)
#     data = fp.read(8)

#     if len(data) != 8:
#         raise ValueError("Invalid data size. Expected 16 bytes.")

#     address = int.from_bytes(data, byteorder='little')
#     data = []
#     for i in range(64):
#         d = fp.read(1)
#         data_value = int.from_bytes(d, byteorder='little')
#         data.append((address + i, data_value))

#     return data



# file_path = 'data.log'
# left_stack = [] # [(address, [])]
# right_stack = []
# address_dict = {} 

# with open(file_path, 'rb') as fp:
#     try:
#         index = 0
#         while True:
#             data_value = parse_binary_file2(fp)
#             data_to_push = []
#             address_push = data_value[0][0]
#             for address, data in data_value:
#                 if address in address_dict:
#                     if address_dict[address] != data:
#                         data_to_push.append((address_dict[address], data))
#                 else:
#                     data_to_push.append((0x0, data))

#             for i, (prev, new) in enumerate(data_to_push, 0):
#                 address_dict[address_push + i] = new
#             left_stack.append((address_push, data_to_push))
#             # index += 1
#     except Exception as e:
#         print(e)
    
# print(left_stack[:20])
# print(f"Size of dict : {len(address_dict)}")


# from array import array

# def parse_binary_file2(fp):
#     # Read 16 bytes (128 bits: 64 bits for address + 64 bits for data)
#     data = fp.read(8)

#     if len(data) != 8:
#         raise ValueError("Invalid data size. Expected 16 bytes.")

#     address = int.from_bytes(data, byteorder='little')
    
#     # Using array for data storage
#     data = array('B')  # 'B' is the format for unsigned 8-bit integers
#     for _ in range(64):
#         d = fp.read(1)
#         if not d:
#             raise ValueError("Unexpected EOF")
#         data_value = d[0]  # `d[0]` is faster than `int.from_bytes()`
#         data.append(data_value)

#     return [(address + i, data[i]) for i in range(64)]


# file_path = 'data.log'
# left_stack = []  # [(address, array('B', []))]
# right_stack = []
# address_dict = {} 

# NUM_TO_PROCESS = 2 ** 20

# with open(file_path, 'rb') as fp:
#     try:
#         index = 0
#         while index <= NUM_TO_PROCESS:
#             data_value = parse_binary_file2(fp)
#             data_to_push = array('B')
#             address_push = data_value[0][0]

#             for address, data in data_value:
#                 prev_data = address_dict.get(address, 0x0)
#                 data_to_push.append(prev_data)
#                 data_to_push.append(data)

#             for i, new in enumerate(data_value):
#                 address_dict[address_push + i] = new[1]

#             left_stack.append((address_push, data_to_push))
#             index += 1
#     except Exception as e:
#         print(e)

# print(left_stack[:20])
# print(f"Size of dict: {len(address_dict)}")





class Entry:
    def __init__(self, addr, val, prev, prev_c, next_c, timestamp) -> None:
        self._address = addr
        self._value = val
        self._prev = prev
        self._prev_child = prev_c
        self._next_child = next_c
        self._timestamp = timestamp


def parse_binary_file(file_path):
    with open(file_path, 'rb') as f:
        # Read 16 bytes (128 bits: 64 bits for address + 64 bits for data)
        data = f.read(16)

        if len(data) != 16:
            raise ValueError("Invalid data size. Expected 16 bytes.")

        address = int.from_bytes(data[:8], byteorder='little')
        data_value = int.from_bytes(data[8:], byteorder='little')

        return address, data_value
    
def parse_binary_file2(fp):
    # Read 16 bytes (128 bits: 64 bits for address + 64 bits for data)
    data = fp.read(16)

    if len(data) != 16:
        raise ValueError("Invalid data size. Expected 16 bytes.")

    address = int.from_bytes(data[:8], byteorder='little')
    data_value = int.from_bytes(data[8:], byteorder='little')

    return address, data_value



file_path = 'data.log'
# address, data_value = parse_binary_file(file_path)

with open(file_path, 'rb') as fp:

    try:
        index = 0
        while True and index <= 10:
            address, data_value = parse_binary_file2(fp)
            print(f"Address: {hex(address)}")
            print(f"Data: {hex(data_value)}")
            index += 1
    except Exception as e:
        print(e)



# from array import array

# class AddressManager:
#     def __init__(self):
#         self.address_dict = {}  # Store current values
#         self.left_stack = []    # Store historical changes (previous values)
#         self.query_history = [] # Store queried addresses
#         self._current_index = -1  # Start at -1 for no queries
#         self.NUM_BLOCK_PROCESS = 2 ** 20

#     def parse_binary_file2(self, fp):
#         """Read and parse the binary file to extract address and data."""
#         data = fp.read(8)

#         if len(data) != 8:
#             raise ValueError("Invalid data size. Expected 16 bytes.")

#         address = int.from_bytes(data, byteorder='little')

#         # Use array for efficient storage of 8-bit integers
#         data = array('B')
#         for _ in range(64):
#             d = fp.read(1)
#             if not d:
#                 raise ValueError("Unexpected EOF")
#             data.append(d[0])  # Directly append byte value

#         return [(address + i, data[i]) for i in range(64)]

#     def load_data_from_file(self, file_path):
#         """Load the data from a binary file and update address values."""
#         with open(file_path, 'rb') as fp:
#             try:
#                 index = 0
#                 while True and index <= self.NUM_BLOCK_PROCESS:
#                     data_value = self.parse_binary_file2(fp)
#                     data_to_push = array('B')  # Store previous and new values
#                     address_push = data_value[0][0]

#                     for address, data in data_value:
#                         prev_data = self.address_dict.get(address, 0x0)  # Get previous value
#                         data_to_push.append(prev_data)  # Store previous value
#                         data_to_push.append(data)       # Store new value

#                     for i, (address, new_data) in enumerate(data_value):
#                         self.address_dict[address] = new_data

#                     self.left_stack.append((address_push, data_to_push))
#                     index += 1
#             except Exception as e:
#                 print(f"Error or End of File: {e}")

#     def query_address(self, address):
#         """Query the current value and previous value of an address."""
#         current_value = self.address_dict.get(address, None)
#         if current_value is None:
#             return f"Address {address} not found."

#         # Search through left_stack to find the previous value
#         previous_value = 0x0  # Default previous value
#         for addr, changes in reversed(self.left_stack):
#             if addr <= address < addr + len(changes) // 2:
#                 idx = (address - addr) * 2
#                 previous_value = changes[idx]
#                 break

#         # Store the query in history for navigation purposes
#         self.query_history.append(address)
#         self._current_index = len(self.query_history) - 1  # Update current index

#         return {
#             "address": address,
#             "current_value": current_value,
#             "previous_value": previous_value,
#         }

#     def history_navigator(self):
#         """A generator to move forward and backward through query history, with previous value lookup."""
#         while True:
#             direction = yield  # Receive 'back' or 'forward'
#             if direction == 'back' and self._current_index > 0:
#                 self._current_index -= 1
#             elif direction == 'forward' and self._current_index < len(self.query_history) - 1:
#                 self._current_index += 1
#             else:
#                 continue

#             address = self.query_history[self._current_index]
#             current_value = self.address_dict.get(address, None)

#             # Search through left_stack to find the previous value
#             previous_value = 0x0  # Default previous value
#             for addr, changes in reversed(self.left_stack):
#                 if addr <= address < addr + len(changes) // 2:
#                     idx = (address - addr) * 2
#                     previous_value = changes[idx]
#                     break

#             # Yield the result with both current and previous values
#             yield {
#                 "address": address,
#                 "current_value": current_value,
#                 "previous_value": previous_value,
#             }

#     def print_addresses(self):
#         """Print all available addresses currently in the address_dict."""
#         if not self.address_dict:
#             print("No addresses available.")
#         else:
#             print("Available addresses:")
#             for address in sorted(self.address_dict):
#                 print(f"Address: {address}")

# # Command-line interface with navigation using generator
# def command_prompt(address_manager):
#     navigator = address_manager.history_navigator()  # Initialize the generator
#     next(navigator)  # Prime the generator

#     while True:
#         command = input("Enter command (query <address>, back, forward, print addresses, quit): ").strip()

#         if command.startswith("query"):
#             try:
#                 address = int(command.split()[1])
#                 result = address_manager.query_address(address)
#                 print(result)
#             except (IndexError, ValueError):
#                 print("Invalid address. Please enter a valid address in integer format.")

#         elif command == "back":
#             try:
#                 result = navigator.send('back')
#                 print(f"Back to address: {result}")
#             except StopIteration:
#                 print("Cannot go further back.")

#         elif command == "forward":
#             try:
#                 result = navigator.send('forward')
#                 print(f"Forward to address: {result}")
#             except StopIteration:
#                 print("Cannot go further forward.")

#         elif command == "print addresses":
#             address_manager.print_addresses()

#         elif command == "quit":
#             print("Exiting.")
#             break

#         else:
#             print("Invalid command. Please use: query <address>, back, forward, print addresses, quit.")


# # Example usage:

# file_path = 'data.log'

# # Instantiate the AddressManager and load data from file
# address_manager = AddressManager()
# address_manager.load_data_from_file(file_path)

# # Start the command prompt interface
# command_prompt(address_manager)





# from tqdm import tqdm
# import psycopg2
# from psycopg2 import extras 
# from array import array
# import os

# class BinaryFileParser:
#     def __init__(self, db_params, batch_size=100, num_block_process=10):
#         self.db_params = db_params
#         self.batch_size = batch_size
#         self.NUM_BLOCK_PROCESS = num_block_process
#         self.conn = psycopg2.connect(**db_params)
#         self.create_table()

#     def create_table(self):
#         """Create table if it doesn't exist in PostgreSQL."""
#         with self.conn.cursor() as cur:
#             cur.execute("""
#                 CREATE TABLE IF NOT EXISTS AddressData (
#                     id BIGSERIAL PRIMARY KEY,
#                     address BIGINT,
#                     data SMALLINT,
#                     timestamp BIGINT
#                 )
#             """)
#             self.conn.commit()

#     def parse_binary_file(self, fp):
#         """Read and parse a 72-byte block from the binary file to extract address and data."""
#         data = fp.read(8)
#         if len(data) != 8:
#             raise ValueError("Invalid data size. Expected 8 bytes for address.")
#         address = int.from_bytes(data, byteorder='little')

#         data = array('B')
#         for _ in range(64):
#             d = fp.read(1)
#             if not d:
#                 raise ValueError("Unexpected EOF")
#             data.append(d[0])
#         return [(address + i, data[i]) for i in range(64)]

#     def load_data_from_file(self, file_path):
#         """Load and process blocks from a binary file and store address values in PostgreSQL."""
#         with open(file_path, 'rb') as fp:
#             file_size = os.path.getsize(file_path)
#             num_blocks = min(file_size // 72, self.NUM_BLOCK_PROCESS)

#             with tqdm(total=num_blocks, desc="Inserting Blocks into Database") as pbar:
#                 batch_data = []
#                 try:
#                     index = 0
#                     timestamp = 0
#                     while index < num_blocks:
#                         data_value = self.parse_binary_file(fp)
#                         batch_data.extend([(address, data, timestamp) for address, data in data_value])

#                         if len(batch_data) >= self.batch_size:
#                             self.insert_batch(batch_data)
#                             batch_data.clear()
#                         pbar.update(1) 
#                         index += 1
#                         timestamp += 1

#                     if batch_data:
#                         self.insert_batch(batch_data)

#                 except Exception as e:
#                     print(f"Error or End of File: {e}")

#     def insert_batch(self, batch_data):
#         """Perform a batch insert into the PostgreSQL database."""
#         with self.conn.cursor() as cur:
#             insert_query = """
#                 INSERT INTO AddressData (address, data, timestamp)
#                 VALUES %s """
#             #     ON CONFLICT (address) DO UPDATE SET data = EXCLUDED.data
#             # """
#             psycopg2.extras.execute_values(cur, insert_query, batch_data)
#             self.conn.commit()

#     def fetch_all_data(self):
#         """Fetch all data entries from PostgreSQL and display progress with tqdm."""
#         with self.conn.cursor() as cur:
#             cur.execute("SELECT COUNT(*) FROM AddressData")
#             total_entries = cur.fetchone()[0]

#             with tqdm(total=total_entries, desc="Fetching Records") as pbar:
#                 cur.execute("SELECT * FROM AddressData")
#                 print("Length : ", len([r for r in cur if r[2] != 0]))
#                 for row in cur:
#                     print(row)
#                     pbar.update(1)

#     def close(self):
#         """Close the database connection."""
#         self.conn.close()


# if __name__ == "__main__":
#     db_params = {
#         'dbname': 'mydatabase',
#         'user': 'myuser',
#         'password': 'mypassword',
#         'host': 'localhost',
#         'port': 5432
#     }

#     parser = BinaryFileParser(db_params=db_params, batch_size=786, num_block_process=2**60)
#     parser.load_data_from_file("../data/data.log")
#     parser.fetch_all_data()
#     parser.close()
