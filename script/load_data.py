from tqdm import tqdm
import psycopg2
from psycopg2 import extras 
from array import array
import os

class BinaryFileParser:
    def __init__(self, db_params, batch_size=100, num_block_process=10):
        self.db_params = db_params
        self.batch_size = batch_size  # Set the batch size for insertion
        self.NUM_BLOCK_PROCESS = num_block_process
        self.conn = psycopg2.connect(**db_params)
        self.create_table()

    def create_table(self):
        """Create table if it doesn't exist in PostgreSQL."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS AddressData (
                    id BIGSERIAL PRIMARY KEY,
                    address BIGINT,
                    data SMALLINT,
                    timestamp BIGINT
                )
            """)
            self.conn.commit()

    def parse_binary_file(self, fp):
        """Read and parse a 72-byte block from the binary file to extract address and data."""
        data = fp.read(8)
        if len(data) != 8:
            raise ValueError("Invalid data size. Expected 8 bytes for address.")
        address = int.from_bytes(data, byteorder='little')

        # Use array for efficient storage of 8-bit integers
        data = array('B')
        for _ in range(64):
            d = fp.read(1)
            if not d:
                raise ValueError("Unexpected EOF")
            data.append(d[0])
        return [(address + i, data[i]) for i in range(64)]

    def load_data_from_file(self, file_path):
        """Load and process blocks from a binary file and store address values in PostgreSQL."""
        with open(file_path, 'rb') as fp:
            file_size = os.path.getsize(file_path)
            num_blocks = min(file_size // 72, self.NUM_BLOCK_PROCESS)  # Determine blocks to process

            with tqdm(total=num_blocks, desc="Inserting Blocks into Database") as pbar:
                batch_data = []  # To hold data for batch insertion
                try:
                    index = 0
                    timestamp = 0
                    while index < num_blocks:
                        data_value = self.parse_binary_file(fp)
                        batch_data.extend([(address, data, timestamp) for address, data in data_value])

                        # Insert in batches
                        if len(batch_data) >= self.batch_size:
                            self.insert_batch(batch_data)
                            batch_data.clear()  # Clear batch after insertion
                        pbar.update(1)  # Update the progress bar after processing each block
                        index += 1
                        timestamp += 1

                    # Insert any remaining data in the batch
                    if batch_data:
                        self.insert_batch(batch_data)

                except Exception as e:
                    print(f"Error or End of File: {e}")

    def insert_batch(self, batch_data):
        """Perform a batch insert into the PostgreSQL database."""
        with self.conn.cursor() as cur:
            insert_query = """
                INSERT INTO AddressData (address, data, timestamp)
                VALUES %s """
            #     ON CONFLICT (address) DO UPDATE SET data = EXCLUDED.data
            # """
            psycopg2.extras.execute_values(cur, insert_query, batch_data)
            self.conn.commit()

    def fetch_all_data(self):
        """Fetch all data entries from PostgreSQL and display progress with tqdm."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM AddressData")
            total_entries = cur.fetchone()[0]  # Get total number of rows

            with tqdm(total=total_entries, desc="Fetching Records") as pbar:
                cur.execute("SELECT * FROM AddressData")
                print("Length : ", len([r for r in cur if r[2] != 0]))
                for row in cur:
                    # Process each row (e.g., print or store in a variable)
                    print(row)
                    pbar.update(1)  # Update the progress bar for each record fetched

    def close(self):
        """Close the database connection."""
        self.conn.close()

# Example usage
if __name__ == "__main__":
    # PostgreSQL database connection parameters
    db_params = {
        'dbname': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'port': 5432
    }

    parser = BinaryFileParser(db_params=db_params, batch_size=786, num_block_process=2**60)
    parser.load_data_from_file("data.log")

    # Fetch all data with progress bar
    parser.fetch_all_data()
    parser.close()
