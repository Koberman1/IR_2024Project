# Wikipedia Search Engine

This Python project implements a search engine for Wikipedia, allowing users to search for queries and retrieve relevant articles.

## Features

- **Search:** Users can search for articles by entering keywords or phrases.
- **Retrieval:** Retrieves 100 relevant Wikipedia article IDs based on the user's search query.
- **Usage:** Open the actual Wikipedia article by copy-paste the result ID into the English Wikipedia site after the `curid=`

   Example:
   ````
   https://en.wikipedia.org/?curid=
   ````

## Installation

1. Clone the repository into a GCP instance:

    ```bash
    https://github.com/Koberman1/IR_2024Project.git
    ```

2. Navigate to the project directory:

    ```bash
    cd IR_2024Project
    ```

3. Create a folder named db
4. Mount the indices from a GCP bucket:
   ```
   gcsfuse --only-dir db -o ro 306-bucket /your/folder/IR_2024Project/db
   ```
5. Run the main Python script:
    ```bash
    nohup python3 search_frontend.py > search.log 2>&1 &
    ```
## Usage
1. Open a browser to the address of your GCP instance public address, on port 8080 and run inline query.
2. For example a "hello world" query:
   ````
   http://YOUR_SERVER_DOMAIN:8080/search?query=hello+world
   ````

## Index Creation and Storage

The indexes of the Wikipedia dump were created offline using the following classes located in the `index` folder:

- **`token_index.py`:** Creates and reads the tokens and creates an index identifier for each. The indexes are stored using SQLite embedded DB because it is based on B-tree which is efficient, and loaded to memory during the run.
- **`title_index.py`:** Creates and reads the page title indices (from their doc ID), loaded into memory during runtime.
- **`tf_idf_index.py`:** Creates and reads the TF-IDF indices of the tokens, loades the indices mapping during runtime and directs to the the data files when query arrives.
- **`page_rank_scores.py`:** Reads a previously created Page Rank scores for the Wikipedia pages and provides information whether an article ID is on a high Page Rank score.

After the indexes were generated, they uploaded into the Google Cloud Storage bucket: `gs://306-bucket`. 

## Utils used:
- **`parquet_utils.py`:** Based on `pyarrow` library to read the preprocessed Wikipedia dumps without using Apache Spark
- **`file_utils.py`:** An inverted index implementation, reads and writes compact binary files for the calculations

## Engine:
- **`engine.py`:** The engine that initiates all the index classes and calculate the result of the query provided
- **`search_frontend.py`:** A web backend based on `Flask` that gets the queries and passes them to the engine

## Contributing

Contributions are welcome! Here's how you can contribute to the project:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature/your-feature`).
3. Make your changes.
4. Commit your changes (`git commit -am 'Add some feature'`).
5. Push to the branch (`git push origin feature/your-feature`).
6. Create a new Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

