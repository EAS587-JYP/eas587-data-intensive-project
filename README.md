# The Attention Span of the World
Semester project for EAS 587: Data Intensive Computing

## Project Overview 

This project studies how global media attention changes following major real-world events.
We use GDELT 2.0 to examine how news coverage rises, reaches a pick and declines over time.

We initally focus on:
- Technology announcements
- Scientific discoveries
- Corporate events
- Politics differ

## Research Question

**How do the attention lifespans of technology announcements, scientific discoveries, corporate events, and politics differ in global news coverage?**

## Datasets 

Primary data set is **GDELT 2.0**.

Core data: 
- **Global Knowledge Graph(GKG)** additional themes and contextual information when useful

Supporting data:
- **Events** information about detected real-world events
- **Mentions** records of media mentions associated with events

## Data Source

GDELT 2.0 public data archive.
GDELT publishes new files every 15 minutes.
For our initial sample, a total of 96 files from each dataset were downloaded, representing approximately 24 hours of data. 
Full raw data is excluded from GitHub because of its size.

## Data Pipeline

Our initial pipeline is:

GDELT 2.0  
→ Acquire raw files  
→ Select relevant event windows  
→ Store and query selected records  
→ Process Events and Mentions  
→ Build media-attention timelines  
→ Calculate attention metrics  
→ Compare event categories

MongoDB is being evaluated as the primary storage system for selected
GDELT records. It allows us to store and query Events and Mentions using
event identifiers and timestamps.

## Follow the instruction to download the data in data/raw/download_instructions.md 

## Repository Structure

```text
eas587-data-intensive-project/
│
├── Phase 1 slide.pdf
├── README.md
├── requirements.txt
├── research_plan.md
│
├── data/
│   ├── samples/
|   |   ├── events/
|   |   ├── gkg/
|   |   └── mentions/
│   └── raw/
|       └── download_instructions.md
|
├── .venv/ 
│
└── src/

# pip install -r requirements.txt