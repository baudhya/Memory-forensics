from tqdm import tqdm
import psycopg2
from psycopg2 import extras
from array import array
import os

class BinaryFileParser:
    def __init__(self, db_params, batch_size=100, num_block_process=10, checkpoint_file='checkpoint.txt'):
        self.db_params = db_params
        self.batch_size = batch_size
        self.NUM_BLOCK_PROCESS = num_block_process
        self.checkpoint_file = checkpoint_file
        self.conn = psycopg2.connect(**db_params)
        self.create_tables()

    def create_tables(self):
        """Create normalized tables if they don't exist in PostgreSQL."""
        with self.conn.cursor() as cur:
            # Create the Addresses table to store unique addresses
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Addresses (
                    id BIGSERIAL PRIMARY KEY,
                    address BIGINT UNIQUE
                )
            """)
            
            # Create the AddressData table to store data and timestamps, linked to Addresses table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS AddressData (
                    id BIGSERIAL PRIMARY KEY,
                    address_id BIGINT REFERENCES Addresses(id),
                    data SMALLINT,
                    timestamp BIGINT
                )
            """)
            
            # Create index on address_id for faster lookups
            # cur.execute("CREATE INDEX IF NOT EXISTS idx_address_id ON AddressData (address_id);")
            self.conn.commit()

    def get_checkpoint(self):
        """Retrieve the last processed block index from the checkpoint file."""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                return int(f.read().strip())
        return 0

    def update_checkpoint(self, last_processed_index):
        """Update the checkpoint file with the last processed block index."""
        with open(self.checkpoint_file, 'w') as f:
            f.write(str(last_processed_index))

    def parse_binary_file(self, fp):
        """Read and parse a 72-byte block from the binary file to extract address and data."""
        data = fp.read(8)
        if len(data) != 8:
            raise ValueError("Invalid data size. Expected 8 bytes for address.")
        address = int.from_bytes(data, byteorder='little')

        # data = array('B')
        # for _ in range(64):
        #     d = fp.read(1)
        #     if not d:
        #         break
        #     data.append(d[0])
        # return [(address + i, data[i]) for i in range(64)]

        data = array('B', fp.read(64))
        length = len(data)
        return [(address + i, data[i]) for i in range(length)]

    def load_data_from_file(self, file_path):
        """Load and process blocks from a binary file and store address values in PostgreSQL."""
        last_processed_index = self.get_checkpoint()
        with open(file_path, 'rb') as fp:
            file_size = os.path.getsize(file_path)
            num_blocks = min(file_size // 72, self.NUM_BLOCK_PROCESS)

            # Move the file pointer to the last processed block
            fp.seek(last_processed_index * 72)

            with tqdm(total=num_blocks, desc="Inserting Blocks into Database", initial=last_processed_index) as pbar:
                batch_data = []
                try:
                    index = last_processed_index
                    timestamp = index
                    while index < num_blocks:
                        data_value = self.parse_binary_file(fp)
                        # Collect address and data together for bulk insertion
                        batch_data.extend([(address, data, timestamp) for address, data in data_value])

                        if len(batch_data) >= self.batch_size:
                            self.insert_batch(batch_data)
                            batch_data.clear()
                            
                            # Update the checkpoint after processing each batch
                            self.update_checkpoint(index + 1)

                        pbar.update(1)
                        index += 1
                        timestamp += 1

                    if batch_data:
                        self.insert_batch(batch_data)
                        self.update_checkpoint(index)
                except Exception as e:
                    print(f"Error or End of File: {e}")



    def insert_batch(self, batch_data):
        """Perform a batch insert of addresses and data into the PostgreSQL database."""
        with self.conn.cursor() as cur:
            # Step 1: Insert addresses directly with ON CONFLICT DO NOTHING
            address_only = [(address,) for address, _, _ in batch_data]
            insert_addresses_query = """
                INSERT INTO Addresses (address) VALUES %s
                ON CONFLICT DO NOTHING
            """
            extras.execute_values(cur, insert_addresses_query, address_only)

            # Step 2: Join Addresses with inserted data for AddressData insertion
            cur.execute("SELECT id, address FROM Addresses WHERE address = ANY(%s);", 
                        (list(set([address for address, _, _ in batch_data])),))
            address_id_map = {address: address_id for address_id, address in cur.fetchall()}

            # Prepare data for AddressData
            address_data_batch = [(address_id_map[address], data, timestamp) 
                                  for address, data, timestamp in batch_data 
                                  if address in address_id_map]
            
            insert_data_query = """
                INSERT INTO AddressData (address_id, data, timestamp)
                VALUES %s
            """
            extras.execute_values(cur, insert_data_query, address_data_batch)
            self.conn.commit()


    def fetch_all_data(self):
        """Fetch all data entries from PostgreSQL and display progress with tqdm."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM AddressData")
            total_entries = cur.fetchone()[0]

            with tqdm(total=total_entries, desc="Fetching Records") as pbar:
                cur.execute("""
                    SELECT Addresses.address, AddressData.data, AddressData.timestamp 
                    FROM AddressData 
                    JOIN Addresses ON AddressData.address_id = Addresses.id
                """)
                for row in cur:
                    print(row)
                    pbar.update(1)

    def close(self):
        """Close the database connection."""
        self.conn.close()

if __name__ == "__main__":
    db_params = {
        # 'dbname': 'mydatabase',
        'database': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'port': 5432
    }

    parser = BinaryFileParser(db_params=db_params, batch_size=2**14, num_block_process=2**60)
    parser.load_data_from_file("../data/data.log")
    parser.fetch_all_data()
    parser.close()
