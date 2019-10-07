<!--
*** Thanks for checking out this README Template. If you have a suggestion that would
*** make this better, please fork the repo and create a pull request or simply open
*** an issue with the tag "enhancement".
*** Thanks again! Now go create something AMAZING! :D
***
***
***
*** To avoid retyping too much info. Do a search and replace for the following:
*** github_username, repo, twitter_handle, email
-->





<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]



<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://github.com/opennebraska/nadc_data">
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">NADC Data</h3>

  <p align="center">
    Scripts to process data from the Nebraska Accountability and Disclosure Commission. The NADC now offers weekly updates of campaign finance data. Every Monday, our script fetches new data, parses it and returns a set of clean files that our app can ingest.
    <br />
    <a href="https://github.com/opennebraska/nadc_data"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/opennebraska/nadc_data">View Demo</a>
    ·
    <a href="https://github.com/opennebraska/nadc_data/issues">Report Bug</a>
    ·
    <a href="https://github.com/opennebraska/nadc_data/issues">Request Feature</a>
  </p>
</p>



<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
  * [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Usage](#usage)
* [Roadmap](#roadmap)
* [Contributing](#contributing)
* [License](#license)
* [Contact](#contact)
* [Acknowledgements](#acknowledgements)



<!-- ABOUT THE PROJECT -->
## About The Project

[![Product Name Screen Shot][product-screenshot]](https://example.com)

## Overview
A standard data dump from the NADC yields 61 pipe-delimited text files (data dictionary at: nadc_tables.rtf). We extract data from 24 of them:

<ul>
<li><strong>Form A1</strong>: Lookup table for most committees</li>
<li><strong>Form A1CAND</strong>: Candidates tied to campaign committees</li>
<li><strong>Form B1</strong>: Campaign statements for candidate or ballot question committees</li>
<li><strong>Form B1AB</strong>: Main table of individual/other contributions</li>
<li><strong>Form B1C</strong>: Loans to campaign committees</li>
<li><strong>Form B1D</strong>: Expenditures by campaign committees</li>
<li><strong>Form B2</strong>: Campaign statements for political party committees</li>
<li><strong>Form B2A</strong>: Contributions to political party committees</li>
<li><strong>Form B2B</strong>: Expenditures by political party committees</li>
<li><strong>Form B4</strong>: Campaign statements for independent committees</li>
<li><strong>Form B4A</strong>: Contributions to independent committees</li>
<li><strong>Form B4B1</strong>: Expenditures by independent committees</li>
<li><strong>Form B4B2</strong>: Federal and out-of-state disbursements</li>
<li><strong>Form B4B3</strong>: Administrative/operating disbursements</li>
<li><strong>Form B5</strong>: Late contributions</li>
<li><strong>Form B6</strong>: Reports of an independent expenditure or donation made by people or entities that are not registered as committees</li>
<li><strong>Form B6EXPEND</strong>: Expenditures made on behalf of committees by people who do not have an ID</li>
<li><strong>Form B7</strong>: Registration of corporations, unions and other associations</li>
<li><strong>Form B72</strong>: Donations by corporations, unions and other associations</li>
<li><strong>Form B73</strong>: Indirect contributions by corporations, unions and other associations</li>
<li><strong>Form B9</strong>: Out of state expenditures/donations</li>
<li><strong>Form B11</strong>: Report of late independent expenditure</li>
</ul>

A shell script, `parser.sh`, makes backups of the raw data and boils down these 24 files (which contain some duplicate transactions) into five data tables:
<ul>
<li><code>toupload/entity.txt</code>: Any group, committee, donor, lender or other entity that has been assigned a unique ID by the NADC. This is the parent table.</li>
<li><code>toupload/donations.txt</code>: Money, inkind donations and pledges.</li>
<li><code>toupload/candidates.txt</code>: Candidates tied to campaign committees and ballot questions.</li>
<li><code>toupload/loans.txt</code>: Lending.</li>
<li><code>toupload/expenditures.txt</code>: Expenditures.</li>
</ul>


Then, the script bakes out some SQL to upload to our live server.

##Handling duplication ###Names NADC has unique identifiers for each donor, but they identify only the address and exact name. If "Matt Wynn" at 1314 Douglas St. gave money, and "Matthew A Wynn" at 1300 Douglas St. gave money, they're considered two different donors.

This is wrong.

We can't deduplicate every donor, so our solution was to create a lookup dictionary (The "CANON" variable in canonical/canonical.py) for any "large" donors, whether in terms of total donations or the number of donations. Super fellow Daniel Wheaton assigned new, real unique identifiers for any of the top 100 donors by both measures. Those lists overlapped a bit, so we wound up deduplicating around 70 donors on the first pass. This is why each entity has two ids, with canonical_id representing our assignment of an identity.

The NADC ID is copied to canonical_id for records that are not deduplicated. Same with the name.

###Donations Some donations are recorded in several places. A late donation, for instance, may also show up as a normal donation record in B1AB.

Donations can also be duplicated within a table, inaccurately. For example, a 1999 ballot committee reported each of its donations twice, leading to a vastly inflated fundraising report.

Susan Lorenz at the Nebraska Accountability and Disclosure Commission said donations of the same amount given on the same day to the same organization are duplicates. Therefore, we deduplicated using those three values.


## Dependencies 

* CSV Kit[](https://csvkit.readthedocs.io/en/latest/)
* Pandas[](https://pandas.pydata.org/)



<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple steps.

### Prerequisites

Make sure to set up a Virtual Env.

* PIP
```sh
pip install csvkit pandas
```

### Installation
 
1. Clone the repo
```sh
git clone https:://github.com/opennebraska/nadc_data.git
```
2. Install PiP packages
```sh
pip install -r requriments.txt
```



<!-- USAGE EXAMPLES -->
## Usage

Use this space to show useful examples of how a project can be used. Additional screenshots, code examples and demos work well in this space. You may also link to more resources.

_For more examples, please refer to the [Documentation](https://example.com)_



<!-- ROADMAP -->
## Roadmap

### Purposeful duplication
To get an exhaustive list of ID'd entities, the script slurps up a half-million records and reduces them to a file about a tenth that size. We use pandas' [`drop_duplicates`](http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.drop_duplicates.html) function, grep and python to ensure to return a set of deduplicated records with the most recent, comprehensive data.

### Data Excluded:
<ul>
<li>Pre-1999 records, which the NADC does not consider especially reliable.</li>
<li>Unpaid pledges, which are recorded as a cash donation if they're actually paid.</li>
<li>Late expenditures in <code>Form B11</code>, which sometimes show up in other tables but cannot reliably be deduplicated. As of Jan. 4, 2016, the universe of late expenditures was 475 records spanning more than a decade and totaling $1.35 million.</li>
<li>Any transaction involving roughly 250 committees and PACs that lack identifying information because of a "glitch" in the NADC's database.</li>
<li>The small number of detail records in<code>Form B6CONT</code>, part of a group of cleanup files for expenditures that aren't recorded elsewhere. It is more trouble than it's worth to pull in.</li>
<li>Out-of-state expenditures and donations listed in <code>Form B9A</code> and <code>Form B9B</code>, which are problematically duplicated in other tables.</li>
<li>Records associated with candidate committee No. 99CAC00167, Friends of Doug Kristensen, which was related to Mr. Kristensen's candidacy for a delegate slot at the 1996 Republican National Convention. As far as we can tell, this is the only time a candidacy for party convention delegate shows up in the data.</li>
</ul>


### Known Issues:
- [ ]  Some records have invalid dates. We added these to a python dictionary in nadc/data/canonical/canonical.py and they get fixed on import.

- [ ] Consider restructuring to fill up the entity_raw.txt first, then run utils dedupeEntity with multithreading.
- [ ] Verify each form is being parsed correctly and each function is outputting correctly.
- [ ] Delete FabFile because we are not using fabric anymore
- [ ] Export pipe delimited data in the temp folder to a database.
- [ ] Update ReadMe
- [ ] Fetch last time script was run and implement constraints on how often to update. (newData Variable )




<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request



<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements

* []()
* []()
* []()





<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/opennebraska/nadc_data.svg?style=flat-square
[contributors-url]: https://github.com/opennebraska/nadc_data/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/opennebraska/nadc_data.svg?style=flat-square
[forks-url]: https://github.com/opennebraska/nadc_data/network/members
[stars-shield]: https://img.shields.io/github/stars/opennebraska/nadc_data.svg?style=flat-square
[stars-url]: https://github.com/opennebraska/nadc_data/stargazers
[issues-shield]: https://img.shields.io/github/issues/opennebraska/nadc_data.svg?style=flat-square
[issues-url]: https://github.com/opennebraska/nadc_data/issues
[license-shield]: https://img.shields.io/github/license/opennebraska/nadc_data.svg?style=flat-square
[license-url]: https://github.com/opennebraska/nadc_data/blob/master/LICENSE.txt
