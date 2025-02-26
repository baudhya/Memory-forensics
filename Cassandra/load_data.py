from tqdm import tqdm
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import os
from array import array

class BinaryFileParserCassandra:
    def __init__(self, host, keyspace, batch_size=100, num_block_process=10, checkpoint_file='checkpoint.txt'):
        self.host = host
        self.keyspace = keyspace
        self.batch_size = batch_size
        self.NUM_BLOCK_PROCESS = num_block_process
        self.checkpoint_file = checkpoint_file
        self.cluster = Cluster([self.host])
        self.session = self.cluster.connect()
        self.session.set_keyspace(self.keyspace)

    def create_table(self):
        """Create a table for storing address data in Cassandra."""
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS address_data (
                address bigint,
                timestamp text,
                value int,
                PRIMARY KEY (address, timestamp)
            )
        """)

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

        data = array('B', fp.read(64))
        length = len(data)
        return [(address + i, data[i]) for i in range(length)]

    def load_data_from_file(self, file_path):
        """Load and process blocks from a binary file and store address values in Cassandra."""
        last_processed_index = self.get_checkpoint()
        with open(file_path, 'rb') as fp:
            file_size = os.path.getsize(file_path)
            num_blocks = max(file_size // 72, self.NUM_BLOCK_PROCESS)

            # Move the file pointer to the last processed block
            fp.seek(last_processed_index * 72)

            with tqdm(total=num_blocks, desc="Inserting Blocks into Database", initial=last_processed_index) as pbar:
                batch_data = []
                try:
                    index = last_processed_index
                    timestamp_counter = index + 1  # Start counter for timestamps as T1, T2, etc.
                    while index < num_blocks:
                        data_value = self.parse_binary_file(fp)
                        for address, value in data_value:
                            timestamp_label = f"T{timestamp_counter}"
                            batch_data.append((address, timestamp_label, value))

                        if len(batch_data) >= self.batch_size:
                            self.bulk_insert(batch_data)
                            batch_data.clear()
                            self.update_checkpoint(index + 1)

                        pbar.update(1)
                        index += 1
                        timestamp_counter += 1

                    if batch_data:
                        self.bulk_insert(batch_data)
                        self.update_checkpoint(index)
                except Exception as e:
                    print(f"Error or End of File: {e}")

    def bulk_insert(self, batch_data):
        """Perform a batch insert of address data into Cassandra."""
        query = """
            INSERT INTO address_data (address, timestamp, value)
            VALUES (%s, %s, %s)
        """
        batch = BatchStatement()
        for address, timestamp, value in batch_data:
            batch.add(query, (address, timestamp, value))
        self.session.execute(batch)

    def close(self):
        """Close the database connection."""
        self.cluster.shutdown()


if __name__ == "__main__":
    # Host should point to the Docker container
    host = "localhost"  # Cassandra is exposed on localhost:9042
    keyspace = "mykeyspace"

    parser = BinaryFileParserCassandra(host=host, keyspace=keyspace, batch_size=50, num_block_process=100000)
    parser.create_table()
    parser.load_data_from_file("./../script/data.log")
    parser.close()
