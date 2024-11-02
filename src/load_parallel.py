from tqdm import tqdm
import psycopg2
from psycopg2 import extras
from array import array
import os
from multiprocessing import Pool, Manager

class BinaryFileParser:
    def __init__(self, db_params, batch_size=100, num_block_process=10, checkpoint_file='checkpoint.txt'):
        self.db_params = db_params
        self.batch_size = batch_size
        self.NUM_BLOCK_PROCESS = num_block_process
        self.checkpoint_file = checkpoint_file
        self.create_tables()

    def create_tables(self):
        """Create normalized tables if they don't exist in PostgreSQL."""

        conn = psycopg2.connect(**self.db_params)
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Addresses (
                    id BIGSERIAL PRIMARY KEY,
                    address BIGINT UNIQUE
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS AddressData (
                    address_id BIGINT REFERENCES Addresses(id),
                    data SMALLINT,
                    timestamp BIGINT
                )
            """)
            
            # Consider dropping index during initial load for faster bulk inserts, then recreating it
            cur.execute("DROP INDEX IF EXISTS idx_address_id;")
            conn.commit()
        conn.close()

    def recreate_index(self):
        """Recreate index on address_id after data has been inserted."""

        conn = psycopg2.connect(**self.db_params)
        with conn.cursor() as cur:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_address_id ON AddressData (address_id);")
            conn.commit()
        conn.close()

    def insert_batch(self, batch_data):
        """Perform a batch insert of addresses and data into the PostgreSQL database."""

        conn = psycopg2.connect(**self.db_params)
        with conn.cursor() as cur:
            address_only = list(set((address,) for address, _, _ in batch_data))
            insert_addresses_query = """
                INSERT INTO Addresses (address) VALUES %s
                ON CONFLICT DO NOTHING
            """
            extras.execute_values(cur, insert_addresses_query, address_only)

            # Step 2: Fetch all address IDs in bulk after insertion
            address_set = list(set([address for address, _, _ in batch_data]))
            cur.execute("SELECT id, address FROM Addresses WHERE address = ANY(%s);", (address_set,))
            address_id_map = {address: address_id for address_id, address in cur.fetchall()}

            # Prepare data for AddressData
            address_data_batch = [(address_id_map[address], data, timestamp)
                                  for address, data, timestamp in batch_data
                                  if address in address_id_map]

            # Bulk insert AddressData
            insert_data_query = """
                INSERT INTO AddressData (address_id, data, timestamp)
                VALUES %s
            """
            extras.execute_values(cur, insert_data_query, address_data_batch)
            conn.commit()
        conn.close()

    @staticmethod
    def parse_binary_file(fp, index):
        """Generator that yields parsed address and data from a binary file."""

        data = fp.read(8)
        if len(data) != 8: return []
        address = int.from_bytes(data, byteorder='little')
        data_array = array('B', fp.read(64))
        return [(address + i, value, index) for i, value in enumerate(data_array)]

    def process_file_part(self, args):
        """Load and process a part of the binary file in a separate process."""

        p, file_path, start, end, total_cache_block = args
        total = end - start + 1
        start_index = start // 72
        total_index = total // 72
        with open(file_path, 'rb') as fp:
            batch = []
            fp.seek(start)
            with tqdm(total=total, desc=f"Processing Blocks : {p}", position=0, leave=False) as pbar:
                for i in range(total_index):
                    batch.extend(BinaryFileParser.parse_binary_file(fp, start_index+i))
                    if len(batch) >= self.batch_size:
                        self.insert_batch(batch)
                        batch.clear()
                    pbar.update(72)
                # Insert any remaining data
                if batch:
                    self.insert_batch(batch)
                pbar.close()

    def parallel_load(self, file_path, num_processes=4):
        file_size = os.path.getsize(file_path)
        address_data_block_size = 72
        total_cache_block = file_size // address_data_block_size
        per_process_cache_blocks = (total_cache_block + num_processes) // num_processes

        offsets = []
        for i in range(num_processes):
            start = i * per_process_cache_blocks * address_data_block_size
            end = min((i+1) * per_process_cache_blocks * address_data_block_size, file_size)
            offsets.append((i, file_path, start, end, total_cache_block))

        # offsets = [(file_path, i * per_process_cache_blocks * address_data_block_size, per_process_cache_blocks, self.batch_size, total_cache_block) for i in range(num_processes)]
        with Pool(processes=num_processes) as pool:
            pool.map(self.process_file_part, offsets)

    def recreate_index(self):
        """Recreate index on address_id after data has been inserted."""

        conn = psycopg2.connect(**self.db_params)
        with conn.cursor() as cur:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_address_id ON AddressData (address_id);")
            conn.commit()
        conn.close()

if __name__ == "__main__":
    db_params = {
        'database': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'port': 5432
    }

    parser = BinaryFileParser(db_params=db_params, batch_size=2**14, num_block_process=2**60)
    parser.parallel_load("../data/data.log", num_processes=32)
    parser.recreate_index()  # Recreate index after the load for faster query operations
