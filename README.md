# pop-tweets

A real-time streaming data analytics system using Apache Storm. It detects the most frequently 
occurring hash tags from the live Twitter data stream in real-time.

_Authors_: [Chen Yuan](https://github.com/yc47084613), [Nick Fabrizio](https://github.com/NFabrizio)

Project is written in Java, and uses Apache Storm for data stream processing.

## Usage

This project is intended to be submitted to a Spark Master via `spark-submit` as a packaged JAR.

- To build, compile, and package the project into a JAR, run `./refresh.sh`
- To submit the built JAR to a Spark Master, edit the Spark job config to your liking and run
    - `./submit.sh <hdfs_input_dir> <hdfs_output_dir>`
- To ingest the raw data into HDFS, use
    - `./ingest.sh <input_data_directory> <hdfs_data_directory>`

## Docs

View the project assignment description under the [docs/](docs) directory
