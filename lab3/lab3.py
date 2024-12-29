import time
import threading
from neo4j import GraphDatabase

class Neo4j():
    def __init__(self, url, login, password):
        self.driver = GraphDatabase.driver(url, auth=(login, password))
        self.driver.verify_connectivity()

    def increment_likes(self, tx):
        query = "MATCH (i:Item {id: 1}) SET i.likes = i.likes + 10000 RETURN i.likes"
        result = tx.run(query)
        return result.single()[0]

    def func(self):
        with self.driver.session() as session:
            session.execute_write(self.increment_likes)

    def likes(self):
        with self.driver.session() as session:
            return session.run("MATCH (i:Item {id: 1}) RETURN i.likes").single()[0]

    def main(self):
        print(f"Likes: {self.likes()}")

        st = time.time()
        threads = []
        for _ in range(10):
            thr = threading.Thread(target=self.func)
            threads.append(thr)
            thr.start()

        for thr in threads:
            thr.join()

        print(f"Time: {int(time.time() - st)}s")
        print(f"Likes: {self.likes()}")

        self.driver.close()


if __name__ == "__main__":
    neo4j = Neo4j("neo4j+ssc://144efa6e.databases.neo4j.io", "neo4j", "d4ph12yVzSlMfhQf7J2l-7VyIavcXs-PFaZqDmB7Gf0")
    neo4j.main()