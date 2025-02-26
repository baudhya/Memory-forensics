from tqdm import tqdm
from pymongo import MongoClient, UpdateOne
import os
from array import array

class BinaryFileParserMongoDB:
    def __init__(self, db_params, batch_size=100, num_block_process=10, checkpoint_file='checkpoint.txt'):
        self.db_params = db_params
        self.batch_size = batch_size
        self.NUM_BLOCK_PROCESS = num_block_process
        self.checkpoint_file = checkpoint_file
        self.client = MongoClient(
            host=db_params['host'],
            port=db_params['port'],
            username=db_params.get('username'),
            password=db_params.get('password'),
            authSource=db_params.get('authSource', 'admin')
        )
        self.db = self.client[db_params['database']]
        self.collection = self.db["AddressData"]

    def create_indexes(self):
        """Create indexes for MongoDB collection."""
        self.collection.create_index("data.timestamp")  # Index for fast queries on timestamps

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
        """Load and process blocks from a binary file and store address values in MongoDB."""
        last_processed_index = self.get_checkpoint()
        with open(file_path, 'rb') as fp:
            file_size = os.path.getsize(file_path)
            num_blocks = max(file_size // 72, self.NUM_BLOCK_PROCESS)

            # Move the file pointer to the last processed block
            fp.seek(last_processed_index * 72)

            with tqdm(total=num_blocks, desc="Inserting Blocks into Database", initial=last_processed_index) as pbar:
                batch_data = {}
                try:
                    index = last_processed_index
                    timestamp_counter = index + 1  # Start counter for timestamps as T1, T2, etc.
                    while index < num_blocks:
                        data_value = self.parse_binary_file(fp)
                        for address, value in data_value:
                            if address not in batch_data:
                                batch_data[address] = []
                            timestamp_label = f"T{timestamp_counter}"
                            batch_data[address].append({'timestamp': timestamp_label, 'value': value})

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
        """Perform a bulk upsert of addresses and their data into MongoDB."""
        bulk_operations = [
            UpdateOne(
                {'_id': address},
                {'$push': {'data': {'$each': data_list}}},
                upsert=True
            )
            for address, data_list in batch_data.items()
        ]
        if bulk_operations:
            self.collection.bulk_write(bulk_operations, ordered=False)

    def fetch_all_data(self):
        """Fetch all data entries from MongoDB and display progress with tqdm."""
        total_entries = self.collection.count_documents({})

        with tqdm(total=total_entries, desc="Fetching Records") as pbar:
            cursor = self.collection.find()
            for doc in cursor:
                print(f"Address: {doc['_id']}, Data: {doc['data']}")
                pbar.update(1)

    def close(self):
        """Close the database connection."""
        self.client.close()


if __name__ == "__main__":
    db_params = {
        'host': 'localhost',
        'port': 27017,
        'username': 'root',
        'password': 'example',
        'authSource': 'admin',  # Specify the authentication database
        'database': 'mydatabase'  # Name of your target database
    }

    parser = BinaryFileParserMongoDB(db_params=db_params, batch_size=1000, num_block_process=100000)
    parser.create_indexes()
    parser.load_data_from_file("../script/data.log")
    parser.fetch_all_data()
    parser.close()
