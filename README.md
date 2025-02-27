# Kafka-Powered Video Metrics System
A distributed event-driven pipeline that tracks YouTube video metrics in real-time. This system monitors video engagement statistics (likes, views, comments) from specified playlists and delivers notifications via Telegram when metrics change.
## Architecture

Data Source: YouTube Data API v3
Message Broker: Apache Kafka (Confluent Cloud)
Serialization: Avro with Schema Registry
Notifications: Telegram API
Language: Python

The system demonstrates implementation of distributed systems principles, event streaming patterns, and cloud-native architectures while providing practical real-time analytics for content creators.
