#!/usr/bin/env python3
import sys
import logging
import requests
import json
from config import config
from pprint import pformat
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient



def fetchPlaylistItemsPage(googleApiKey, youtubePlaylistID, pageToken = None):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params= {
        "key": googleApiKey,
        "playlistId" : youtubePlaylistID,
        "part" : "contentDetails",
        "pageToken":  pageToken,
        })
    
    payload = json.loads(response.text)

    logging.debug("GOT %s", payload)

    return payload

def fetchPlaylistItems(googleApiKey, youtubePlaylistID, pageToken=None):
    payload = fetchPlaylistItemsPage(googleApiKey, youtubePlaylistID, pageToken)

    yield from payload["items"]

    nextPageToken = payload.get("nextPageToken")

    if(nextPageToken) is not None:
        fetchPlaylistItems(googleApiKey, youtubePlaylistID, nextPageToken)

def fetchVideos(googleApiKey, youtubePlaylistID, pageToken=None):
    payload = fetchVideosPage(googleApiKey, youtubePlaylistID, pageToken)

    yield from payload["items"]

    nextPageToken = payload.get("nextPageToken")

    if(nextPageToken) is not None:
        fetchVideos(googleApiKey, youtubePlaylistID, nextPageToken)


def fetchVideosPage(googleApiKey, videoId, pageToken = None):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos", params= {
        "key": googleApiKey,
        "id" : videoId,
        "part" : "snippet,statistics",
        "pageToken":  pageToken,
        })
    
    payload = json.loads(response.text)

    logging.debug("GOT %s", payload)

    return payload

def summarize(videos):
    return{
        "videoId" : videos["id"],
        "title" : videos["snippet"]["title"],
        "views" : int(videos["statistics"].get("viewCount", 0)),
        "likes" : int(videos["statistics"].get("likeCount", 0)), 
        "comments" : int(videos["statistics"].get("commentCount", 0)),

    }

def onDelivery(err, record):
    pass



def main():
    logging.info("START")
    googleApiKey = config["googleApiKey"]
    youtubePlaylistID = config["youtubePlaylistID"]
    schema_registry_client = SchemaRegistryClient(config["schema_registry"])

    youtubeVideosValuesSchema = schema_registry_client.get_latest_version("youtubeVideos-value")

    kafkaConfig = config["kafka"] | {
        "key.serializer": StringSerializer(),
        "value.serializer" : AvroSerializer(schema_registry_client, youtubeVideosValuesSchema.schema.schema_str),
    }
    producer = SerializingProducer(kafkaConfig)

    for videoItems in  fetchPlaylistItems(googleApiKey, youtubePlaylistID):
        videoId = videoItems["contentDetails"]["videoId"]
        for videos in fetchVideos(googleApiKey, videoId):
            logging.info("GOT %s", pformat(summarize(videos)))

            producer.produce(
                topic="youtubeVideos",
                key = videoId,
                value = {
                    "TITLE" : videos["snippet"]["title"],
                    "VIEWS" : int(videos["statistics"].get("viewCount", 0)),
                    "LIKES" : int(videos["statistics"].get("likeCount", 0)), 
                    "COMMENTS" : int(videos["statistics"].get("commentCount", 0)),

                },
                on_delivery = onDelivery
            )
    producer.flush()



if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO)
    sys.exit(main())




