# Gospel Study in Obsidian
I intend to use this as a means to document how I set up and study the Gospel in [Obsidian](https://obsidian.md).

My goal is to create JSON files by converting or scraping scriptures, conference talks, and other manuals. I will then generate a list of cross-references or other resources that can be added to the markdown files I study in Obsidian. 

To these ends, the scripts will be named by what they produce. `json_conference.py` will scrape the conference input and output a corresponding JSON file. `add_conference_source.py` will then add a source to each talk in the specified general conference files in the output folder. 

## Output Folders
### json_scriptures
Included with this template. I didn't use any copywritten versions of the LDS scriptures so I could include these json files. As a result there are no footnotes that are typically found in the scriptures. 

### json_conference
Not included. You will need to scrape this from the internet on your own (tbh I feel like I could have it here within their )

### Scriptures

### Conference

## Generation Files
### add_scripture_resource.py
There can be various of these functions included. 

### ai_scripture_resource.py


### scrape_conference.py
Scrapes the LDS conference and adds a few of the easier (have't or rarely error) resources. Uses the selenium project so you will need to be sure it's installed to run the python command.

### md_scripture_resource_updater.py
This is a multi purpose script that either creates the scriptures_json folder with the markdown files in it or updates the headings of the markdown files so that any footnotes, highlights, etc. are preserved. Run it to create the markdown files or run it to update the resources on existing markdown files. 
