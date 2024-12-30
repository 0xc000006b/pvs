import threading
import time
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern


class MongoDB:
    def __init__(self, url):
        self.url = url
        self.client = MongoClient(url)

    def thread(self, write_concern, increments):
        client = MongoClient(self.url)
        db = client["performance_test"]
        collection = db.get_collection("likes_counter", write_concern=write_concern)

        [collection.find_one_and_update({"_id": 1}, {"$inc": {"likes": 1}}) for _ in range(increments)]
           
        client.close()

    def test(self, write_concern, increments, clients):
        threads = []
        for _ in range(clients):
            thread = threading.Thread(target=self.thread, args=(write_concern, increments))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def main(self, increments_per_client, clients_count):

        db = self.client["performance_test"]

        db.likes_counter.update_one({"_id": 1}, {"$set": {"likes": 0}})
        print("writeConcern = 1")
        st = time.time()
        test(WriteConcern(w=1), increments_per_client, clients_count)
        print(f"Time: {int(time.time() - st)}s")

        likes_count = db.likes_counter.find_one({"_id": 1})["likes"]
        print("Likes:", likes_count)

        db.likes_counter.update_one({"_id": 1}, {"$set": {"likes": 0}})
        print("Likes reset to 0.")

        print("\nwriteConcern = majority")
        st = time.time()
        test(WriteConcern(w="majority"), increments_per_client, clients_count)
        print(f"Time: {int(time.time() - st)}s")

        likes_count = db.likes_counter.find_one({"_id": 1})["likes"]
        print("Likes:", likes_count)

        self.client.close()


if __name__ == "__main__":
    mongo = MongoDB("mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0")
    mongo.main(10000, 10)