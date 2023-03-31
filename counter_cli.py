# Utility to count the number of new kotus words in the database jsonl file
# setup command line arguments
import argparse
import datetime
import json
import os
import subprocess
from google.cloud import storage


# main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Count the number of new kotus words in the database jsonl file"
    )
    parser.add_argument("--jsonl_file", help="jsonl file to count")
    # argument for the unique words file in the bucket (unique_words.json)
    parser.add_argument("--unique_words", help="unique words file in the bucket")
    # bucket name
    parser.add_argument("--bucket", help="bucket name")
    # output bucket name
    parser.add_argument("--output_bucket", help="output bucket name")
    args = parser.parse_args()

    # check that bucket name is provided
    if not args.bucket:
        print("Bucket name is required")
        # print usage
        parser.print_usage()
        exit()

    # check that jsonl file is provided
    if not args.jsonl_file:
        print("jsonl file is required")
        # print usage
        parser.print_usage()
        exit()

    # check that unique words file is provided
    if not args.unique_words:
        print("unique words file is required")
        # print usage
        parser.print_usage()
        exit()

    # check that output bucket name is provided
    if not args.output_bucket:
        print("output bucket name is required")
        # print usage
        parser.print_usage()
        exit()

    # assert that bucket is not the same as the output bucket
    assert (
        args.bucket != args.output_bucket
    ), "Bucket and output bucket cannot be the same"

    # create a GCP storage client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(args.bucket)
    output_bucket = storage_client.get_bucket(args.output_bucket)

    # check if the downloads directory exists
    # if not create it using os.mkdir
    # if it exists do nothing
    if not os.path.exists("downloads"):
        os.mkdir("downloads")

    # check if the unique words file exists in the output bucket
    # we will create it and upload it to the bucket in the end if it does not exist
    stats = storage.Blob(bucket=output_bucket, name=args.unique_words).exists(
        storage_client
    )

    # download the unique words file from the GCP bucket
    if stats:
        # download the unique words file using gsutil to downloads directory
        subprocess.run(
            [
                "gsutil",
                "cp",
                "gs://" + args.output_bucket + "/" + args.unique_words,
                "downloads/" + args.unique_words,
            ]
        )

    # create the unique words set from contents of the unique words file
    # if the file does not exist create an empty set
    # unique word file is a json constructed as follows
    # file object contains the filename, date, and new word count of the jsonl file
    # {"files": [{"filename": "filename.jsonl", "date": "2020-01-01", "new_words": 1000}"}], "words": ["word1", "word2", "word3"]}
    # the files array contains the names of the jsonl files that have been processed
    # the words array contains the unique words in the jsonl files
    unique_words = {}
    if stats:
        with open("downloads/" + args.unique_words, "r") as f:
            unique_words = json.load(f)

        # create a set from the words array
        unique_words_set = set(unique_words["words"])
        # create a set from the files array
        unique_files_set = set([file["filename"] for file in unique_words["files"]])
    else:
        unique_words_set = set()
        unique_files_set = set()

        # check if the jsonl file has been processed
        # if it has been processed do nothing
        # if it has not been processed add it to the files array
    if args.jsonl_file not in unique_files_set:
        unique_files_set.add(args.jsonl_file)
    else:
        print("File has already been processed")
        exit()

    # record the number of unique words before processing the jsonl file
    unique_words_before = len(unique_words_set)

    # download the jsonl file from the GCP bucket
    # download the jsonl file using gsutil to downloads directory
    # if the file already exists in the downloads directory do nothing
    # if the file does not exist in the downloads directory download it
    if not os.path.exists("downloads/" + args.jsonl_file):
        subprocess.run(
            [
                "gsutil",
                "cp",
                "gs://" + args.bucket + "/" + args.jsonl_file,
                "downloads/" + args.jsonl_file,
            ]
        )
    else:
        print("File already exists in downloads directory")

    # open the jsonl file and read each line
    # for each line check if the word is in the unique words set
    # if it is not in the set add it to the set
    # if it is in the set do nothing
    with open("downloads/" + args.jsonl_file, "r") as f:
        for line in f:
            word = json.loads(line)["BOOKWORD"]
            if word not in unique_words_set:
                unique_words_set.add(word)

    # record the number of unique words after processing the jsonl file
    unique_words_after = len(unique_words_set)

    # the number of new words is the difference between the number of unique words before and after
    new_words = unique_words_after - unique_words_before
    # utc date of the jsonl file from the filename which is a epoch timestamp
    date_part = args.jsonl_file.split(".")[0]
    try:
        timestamp = datetime.datetime.fromtimestamp(int(date_part))
    except ValueError:
        timestamp = datetime.datetime.fromisoformat("2023-01-01")

    file_stats = {
        "filename": args.jsonl_file,
        "date": timestamp.isoformat(),
        "new_words": new_words,
    }

    # add the file stats to the files array
    # ensure that the files array exists
    if "files" not in unique_words:
        unique_words["files"] = []
    unique_words["files"].append(file_stats)

    # print the number of new words in the jsonl file
    print(
        f"Number of new words in the {args.jsonl_file} file: {unique_words_after - unique_words_before}"
    )

    # create a dictionary with the files array and words array
    unique_words = {"files": unique_words["files"], "words": list(unique_words_set)}

    # upload the unique words file to the GCP bucket
    # upload the unique words file using gsutil from downloads directory
    with open("downloads/" + args.unique_words, "w") as f:
        json.dump(unique_words, f)

    subprocess.run(
        [
            "gsutil",
            "cp",
            "downloads/" + args.unique_words,
            "gs://" + args.output_bucket + "/" + args.unique_words,
        ]
    )
