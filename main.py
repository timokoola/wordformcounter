import os
import functions_framework
from google.cloud import storage
import json
import datetime


# Triggered by a change to a Cloud Storage bucket.
@functions_framework.cloud_event
def counter(cloud_event):
    data = cloud_event.data

    event_type = cloud_event["type"]

    # if the event type is not OBJECT_FINALIZE
    # do nothing
    if "finalize" not in event_type:
        return

    bucket = data["bucket"]

    name = data["name"]

    if "jsonl" not in name:
        return

    # create a GCP storage client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)

    # get output bucket name from the environment variable
    output_bucket_name = os.environ.get("OUTPUT_BUCKET")

    # assert that the output bucket name is not the same as the bucket
    assert (
        output_bucket_name != bucket.name
    ), "Bucket and output bucket cannot be the same"

    # get the output bucket
    output_bucket = storage_client.get_bucket(output_bucket_name)

    # check that the unique words file exists in the bucket
    stats = storage.Blob(bucket=output_bucket, name="unique_words.json").exists(
        storage_client
    )

    if not stats:
        # print error message
        print("unique_words.json does not exist in the bucket")
        return

    # get the unique words file from the bucket

    blob = output_bucket.blob("unique_words.json")

    # download the unique words file to current directory
    blob.download_to_filename("unique_words.json")
    # open the unique words file
    with open("unique_words.json", "r") as f:
        unique_words = json.load(f)

    # create a set from the words array
    unique_words_set = set(unique_words["words"])
    # create a set from the files array
    unique_files_set = set([file["filename"] for file in unique_words["files"]])

    # check if the jsonl file has been processed
    # if it has been processed do nothing
    # if it has not been processed add it to the files array
    if name not in unique_files_set:
        unique_files_set.add(name)
    else:
        print("File has already been processed")
        exit()

    # record the number of unique words before processing the jsonl file
    unique_words_before = len(unique_words_set)

    # download the jsonl file from the GCP bucket
    jsonl_blob = bucket.blob(name)
    jsonl_blob.download_to_filename(name)

    # open the jsonl file and read each line
    # for each line check if the word is in the unique words set
    # if it is not in the set add it to the set
    # if it is in the set do nothing
    with open(name, "r") as f:
        for line in f:
            word = json.loads(line)["BOOKWORD"]
            if word not in unique_words_set:
                unique_words_set.add(word)

    # record the number of unique words after processing the jsonl file
    unique_words_after = len(unique_words_set)

    # the number of new words is the difference between the number of unique words before and after
    new_words = unique_words_after - unique_words_before
    # utc date of the jsonl file from the filename which is a epoch timestamp
    date_part = name.split(".")[0]
    try:
        timestamp = datetime.datetime.fromtimestamp(int(date_part))
    except ValueError:
        timestamp = datetime.datetime.fromisoformat("2023-01-01")

    file_stats = {
        "filename": name,
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
        f"Number of new words in the {name} file: {unique_words_after - unique_words_before}"
    )

    # create a dictionary with the files array and words array
    unique_words = {"files": unique_words["files"], "words": list(unique_words_set)}

    # upload the unique words file to the GCP bucket
    # upload the unique words file using gsutil from downloads directory
    with open("unique_words.json", "w") as f:
        json.dump(unique_words, f)

    # upload the unique words file to the GCP  outputbucket
    upload_blob = output_bucket.blob("unique_words.json")
    upload_blob.upload_from_filename("unique_words.json")
