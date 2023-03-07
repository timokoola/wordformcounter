# wordformcounter

Utility and GCP Cloud function to count the number of new words for each analysis file

## Command line Utility

Usage of the command line Utility

```bash
usage: counter_cli.py [-h] [--jsonl_file JSONL_FILE] [--unique_words UNIQUE_WORDS] [--bucket BUCKET]

Count the number of new kotus words in the database jsonl file

options:
  -h, --help            show this help message and exit
  --jsonl_file JSONL_FILE
                        jsonl file to count
  --unique_words UNIQUE_WORDS
                        unique words file in the bucket
  --bucket BUCKET       bucket name
```

## GCP cloud function
