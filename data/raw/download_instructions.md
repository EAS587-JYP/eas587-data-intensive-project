# Download Instructions

- Raw data is obtained from GDELT 2.0 using gdeltloader following the steps given below:

1. Install gdeltloader:
    python -m pip install gdelttools

2. GKG:
    gdeltloader --master --update --download --last 96 --filter gkg

3. Events:
    gdeltloader --master --update --download --last 96 --filter export

4. Mentions:
    gdeltloader --master --update --download --last 96 --filter mentions

# Additional Info:
URL: https://gdeltproject.org/data.html