# The Attention Span of the World

## Cover Page

**Research Project Title:**  
The Attention Span of the World: Measuring the Lifespan of Global Media Attention with GDELT

**Created By:**  
Jake Rah, Prasansha Bharati, Yokibha Chapagain

**Version:** 1.0

**Target Community of Interest:**  
Media researchers, journalists, information scientists, and researchers studying news diffusion.

**Date Created:** 
September 2026

**Last Updated:** 
September 17, 2026

**GitHub Repository:**  
https://github.com/EAS587-JYP/eas587-data-intensive-project

# 1. Research Goal
Some news spikes within hours, gets picked up across dozens of countries, and then disappears in two days. Other stories build slowly and stay in circulation for weeks without ever traveling far from where they happened. Not much of this is actually measured. How long a story "lasts" tends to get described anecdotally rather than quantified, which is a little strange given how much news data is now available.
This project treats attention lifespan as a measurable thing. We plan to use the GDELT 2.0 Events and Mentions data, which records every media mention of a coded news event along with a timestamp, to build an attention curve for each event. The curve tells us how many outlets covered it, when, for how long, and from where. From those curves we pull out a small set of comparable measures: peak coverage, how fast it rose, how fast it declined, how far it spread geographically, and an attention half-life. Then we compare how those measures differ across kinds of events. What we expect to end up with is a data-driven picture of what holds the world's attention and what gets forgotten quickly, plus some natural groupings of events that share similar attention shapes.

## Primary Research Question
- How do the attention lifespans of technology announcements, scientific discoveries, corporate events, and politics differ in global news coverage?
- What makes some events spread fast and fade fast while others last?

## Supporting Questions
1. How quickly does media attention rise and reach its peak after an event occurs?
2. Does media sentiment become more negative as a geopolitical crisis escalates?
3. Do technology announcements, scientific discoveries and corporate events show different patterns of attention duration and geographic spread?
4. Does the tone of coverage shift as an event gets older?
5. Does the size of the initial spike predict the length of the tail? Do bigger events fade faster or slower?

# 2. Background and Motivation
Media reporting frequently shifts quickly following significant events in the real world. Certain events attract brief attention, whereas others stay prominent in the media for days or weeks. Grasping these patterns can assist in depicting how information circulates within the global news network.

**GDELT** is appropriate for this study as it offers extensive structured data on worldwide news incidents and their media references. GDELT 2.0 delivers distinct Event and Mentions data streams every 15 minutes, enabling the analysis of media coverage across time. The project intends to utilize this temporal framework to assess and contrast the longevity of media focus within different event categories.

# 3. Research Objectives and Scope
## Objectives
- Get access to GDELT 2.0 at scale and set up a repeatable way to download, subset, and store a working slice of it. 
Expected outcome: a working ingestion pipeline and a documented description of the data's size and structure.
- Build attention curves for a large number of events by grouping mentions into time bins. 
Expected outcome: a set of per-event curves covering a decent slice of recent news.
- Extract comparable measures from each curve, including peak, time to peak, decline rate, attention half-life, total coverage, and geographic spread. 
Expected outcome: a feature set with one row per event.
- Group events by the shape of their attention curves using unsupervised learning. 
Expected outcome: a small number of interpretable attention profiles that we can describe and name.
- Compare those groups against event metadata to see what kinds of events land in which profile. 
Expected outcome: a characterization of which event types are short-lived and which ones persist.

## In Scope
- GDELT 2.0 Events data, used to identify what happened and to supply metadata
- GDELT 2.0 Mentions data, used as the main measurement of when and how often something was covered
- Selected GKG fields as supplementary context on themes and organizations
- Aggregating mentions over time to build attention curves
- Attention measures: peak coverage, time to peak, decline rate, attention half-life, coverage duration
- Geographic spread of coverage
- Tone of coverage over time
- Unsupervised grouping and comparison of attention curves

## Out of Scope
- Individual reader attention. GDELT measures what was published, not what anyone actually read.
- Social media attention. No platform engagement data.
- Judging whether coverage is accurate.
- Causal claims about why journalists chose to cover something.
- Full article text, which GDELT does not redistribute.

# 4. Prior Research and References
Crane and Sornette [1] studied how view counts on YouTube videos rose and fell after posting, and showed that the shape of that curve alone, without knowing anything else about the video, can tell an outside shock (like a video getting linked from a major news site) apart from something that built up gradually through word of mouth. This is close to our reasoning for clustering by shape instead of assigning labels ahead of time.

Candia et al. [2] looked at how attention fades for several very different things at once, including citations to academic papers, patents, song plays, and Wikipedia page views for biographies. They found a consistent two-phase decay pattern across all of them: a fast initial drop followed by a slower, longer decline. We want to check whether news coverage follows a similar pattern.

Lorenz-Spreen et al. [3] studied attention data going back decades, including things like hashtags, search terms, and movie ticket sales, and found that attention peaks have gotten steeper and shorter over time. They explained this as topics competing for a limited amount of collective attention, so as more content gets produced, each individual story gets a shorter turn in the spotlight. This connects to our question about whether large events crowd out smaller ones.

Tsai and Wu [4] used the Mashable news dataset, a well-known public dataset of article metadata and share counts from the media site Mashable, to predict how many times an article would be shared using several machine learning models. This is a useful contrast for us: they predict a single popularity number for one article, while we are measuring a full trajectory across many outlets over time.

Leetaru and Schrodt [5] introduced GDELT itself and explained its approach to automatically coding news articles from around the world into structured events, which is the basis for the GKG, Events, and Mentions data we use. Tukey [6] is the classic reference on exploratory data analysis and guides our approach to exploring the data in Phase 2.

### References:
[1] R. Crane and D. Sornette, "Robust dynamic classes revealed by measuring the response function of a social system," Proceedings of the National Academy of Sciences, vol. 105, no. 41, pp. 15649–15653, Oct. 2008.

[2] C. Candia, C. Jara-Figueroa, C. Rodriguez-Sickert, A.-L. Barabási, and C. A. Hidalgo, "The universal decay of collective memory and attention," Nature Human Behaviour, vol. 3, no. 1, pp. 82–91, Jan. 2019.

[3] P. Lorenz-Spreen, B. M. Mønsted, P. Hövel, and S. Lehmann, "Accelerating dynamics of collective attention," Nature Communications, vol. 10, art. no. 1759, Apr. 2019.

[4] M.-J. Tsai and Y.-Q. Wu, "Predicting online news popularity based on machine learning," Computers & Electrical Engineering, vol. 102, art. no. 108198, Sep. 2022.

[5] K. Leetaru and P. A. Schrodt, "GDELT: Global data on events, location and tone, 1979–2012," in Proc. International Studies Association Annual Convention, San Francisco, CA, USA, 2013.

[6] J. W. Tukey, Exploratory Data Analysis. Reading, MA, USA: Addison-Wesley, 1977.

# 5. Supporting Data and Resources
## Primary Dataset: **GDELT 2.0**
Our primary dataset is **GDELT 2.0**. It is free, needs no registration, and is available from the GDELT Project's file server as well as through a Google BigQuery mirror. It comes as compressed tab-delimited files, one per stream every 15 minutes, going back to February 2015. GDELT's own documentation says that 2015 alone involved over 2.5 TB of raw GKG data, and the project has grown a lot since then. By 2018, GDELT reported that the GKG had processed close to 800 million articles from 2015 onward, amounting to 22 billion data fields. The full corpus across the Events and Mentions streams runs into terabytes uncompressed, which is well beyond what any of our machines could hold in memory. That is exactly why this project needs sampling and subsetting rather than loading the data whole.

The **Events data** gives us the structured record of what happened: an event ID, dates, the actors involved, an event code, a location, source counts, and an average tone. It has 61 columns. We use it to identify events and to supply the metadata that we later compare our findings against.

The **Mentions data** is our main measurement instrument. It has 16 columns and records every individual media mention of every event, each with its own timestamp, source name, document tone, and confidence score. Because it stores each mention separately instead of summarizing them, it works as a ready-made time series of coverage for every event in the system. 
This is what we use to build the attention curves and to calculate peak coverage, time to peak, coverage duration, and attention half-life.

The **GKG data** adds themes, organizations, people, locations, and tone associated with coverage. We treat it as enrichment for context rather than a core source, since it is a lot larger per record.

Our sampling approach is to work with a recent contiguous window of the data instead of the whole archive, read only the columns we actually need, keep only events with enough mentions to form a usable curve, and convert what survives into a columnar format so repeated analysis is faster. A single day of Mentions is already around a million rows, making it necessary for us to sample and categorize according to our research needs to keep this project within the scope of our resources.

Beyond the data itself, we need Python with pandas, NumPy, SciPy, and scikit-learn for the analysis and clustering, Matplotlib for visualization, Apache Spark for the distributed processing in Phase 3, and GitHub for version control. All of it is freely available, and we have already confirmed that the GDELT files download without any access restrictions.


# 6. Risks, Constraints, Assumptions, and Open Questions
## Risks
- The dataset is very large, so downloading and storing enough of it could take longer than we expect. Our plan is to work with a limited time window first and widen it only if the timing allows.
- GDELT sometimes records the same real-world happening as several separate events. If we do not handle this, one attention curve gets split into fragments and our duration measures come out wrong. We plan to group closely related events together before building curves.
- GDELT's event coding scheme was designed mainly around political and conflict events, so other kinds of events may be coded less cleanly. This is part of why we are grouping by curve shape instead of relying on predefined labels.
- Coverage in GDELT leans toward English-language and Western outlets, which could distort our measure of geographic spread. We will report the bias rather than assume it away.
- Events near the end of our time window will have their tails cut off, which makes them look shorter-lived than they really were. We will leave a buffer at the end of the window to deal with this.

## Constraints 
Roughly twelve weeks of time, ordinary laptop hardware with limited memory and disk space, 
a team working part-time alongside other coursework, and no control at all over how GDELT codes its data.

## Assumptions
The number of mentions over time is a reasonable proxy for media attention, that GDELT's mention timestamps are accurate enough to measure how fast coverage rises, that most attention curves have one clear peak so half-life is well defined, and that source names can be mapped to countries well enough to measure spread. All of these can be checked against the data, and if one of them turns out to be wrong, that is a finding worth reporting rather than a failure.

## Open Questions
1. What time bin is the right one. Fifteen minutes keeps the detail but is noisy, while daily bins are stable but may blur out fast-moving stories?
2. How many mentions an event should have before we consider its curve usable?
3. How many natural groups exist in the attention curves, and whether those groups turn out to be interpretable at all?
4. Whether news events follow the same two-phase decay found for other kinds of content, or something different?
5. Whether simultaneous big events crowd each other out, and if so whether we need to control for that?

# 7. Research Approach, Tasks, and Timeline
## Research Approach 
Our method is cyclical instead of sequential. We aim to validate the data and execute the entire pipeline from start to finish on a small sample initially, such as one month, to ensure that every phase from data retrieval to curve creation to clustering generates output. Only then do we expand the window. Addressing it this way allows an issue in the pipeline to emerge in week two rather than week eight, ensuring we consistently have something presentable at every checkpoint.

We initially focus on four event categories to evaluate whether different types of events exhibit different media-attention lifespans. The categories we have chosen are:

1. Technology announcement:
- Major product launch
- AI/model release
- New hardware announcement
- Major platform launch
- Scientific discoveries
- Major research finding 
- Space/science discovery
- Medical/scientific breakthrough

2. Corporate events
- Merger/acquisition
- Bankruptcy
- Major earnings event
- Major leadership change
- Major corporate announcement

3. Political events
- Upcoming or result of elections
- Funding from government
- Announcement of or ongoing war 
- Political agreements


## Timeline
**Week 1 (Sep 8–17):** Define the research question and scope, set up the GitHub repository, 
write the download script, verify the data is accessible at the scale we need, and pull 
representative samples. Deliverables are the research plan, the repository, the data samples, 
and the workshop slides and report.

**Week 2 (Sep 18–24):** Build the ingestion pipeline. Download a one-month slice of Events and Mentions, keep only the columns we need, and store it in a form we can query repeatedly. Get a first look at how many events and mentions actually survive the filtering.

**Week 3 (Sep 25–Oct 1):** Clean the data. Handle duplicates, nulls, low-confidence mentions, and the problem of one real event being coded several times over. Build the first attention curves and inspect them by hand against events we recognize, just to make sure the curves look sensible.

**Week 4 (Oct 2–8):** Extract the measures from each curve, so peak, time to peak, decline rate, half-life, duration, and geographic spread. Carry out exploratory data analysis. Start the first unsupervised grouping of curve shapes.

**Week 5 (Oct 9–16):** Pull the exploratory findings together and build presentation-quality visualizations of the attention curves and their groupings. Phase 2 data-analysis presentation, October 14–16.

**Weeks 6–7 (Oct 17–30):** Refine the clustering, settle on the number of groups, and characterize each one. Compare the groups against event metadata to see which kinds of events land where. Look at how scale, spread, and duration relate to each other.

**Weeks 8–9 (Oct 31–Nov 13):** Move the pipeline to Apache Spark and widen the time window, then check whether the results still hold at the larger scale.

**Weeks 10–11 (Nov 14–27):** Enrich the analysis with GKG themes, add tone trajectories to the group descriptions, and run robustness checks on our choices of time bin and thresholds.

**Week 12 (Nov 30–Dec 5):** Finalize results, write up the findings and the limitations, clean up the repository so it runs in a fresh environment, and build the final presentation.

**Week 13 (Dec 7–11):** Final presentation.

# Design Decisions
**GKG** as the primary dataset. It is the largest and richest of the three GDELT streams, with tone, themes, people, organizations, and locations tagged on nearly every document. It is also not pre-cleaned or deduplicated (GKG 2.1 gives every article its own row instead of grouping identical ones together), so the work of filtering, sampling, and making sense of it is a major part of our process.

**Events and Mentions** as a supporting layer. Events gives us structured actor and location metadata for validation, and Mentions gives a per-article timestamp and tone we can cross-check against what we build from GKG. Both are smaller and easier to query than GKG, which makes them useful for grounding our results, but neither carries enough thematic or emotional detail on its own to be the main measurement instrument.

GDELT only records what news organizations published, not who actually read it or how they reacted. We will be careful to frame every result as a claim about coverage volume and tone, not about audience behavior, so we do not overstate what the data can tell us.

**Attention half-life** as the main measure. Unlike a raw count of mentions, half-life is defined relative to each curve's own peak, so a huge global story and a small regional one can be compared on the same scale instead of the huge story simply dominating every summary statistic. It follows decay-focused framing, used in the prior literature we are building on, which makes our results easier to compare against theirs.

Before running the pipeline on the full time window, we will get every step, from download to cleaning to curve-building to clustering, working end to end on a single month of data. This way, if something in the design is wrong, it will show up early, while it is still cheap to fix.