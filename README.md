# popest-bk20

<em>Summary:</em><br>
This repository include data and code to demonstrate methods for estimating 2020 US census block population by race/ethnicity with reference to the 2020 Census state apportionment total only. 

<em>Background:</em><br>
The 2020 US Census faced by challenges on many fronts: politicization of the census, new address canvassing, enumeration, non-response followup, and disclosure avoidance methods, and a global pandemic. All of this in a context of widespread decline in survey participation rates. Delays in enumeration and processing of the Census results created a situation in which some states were uncertain to meet legal requirements to complete redistricting based on the 2020 Census. The National Conference of State Legislatures suggested that one option could be to do an initial sweep of redistricting with "the best data available at present" <a href="https://www.ncsl.org/research/redistricting/5-ways-to-handle-census-delays-and-redistricting-deadlines-magazine2021.aspx">(link)</a>. On my view, the best source of data about 2020 came from materials disseminated through partnership programs designed to strengthen and support the 2020 Census effort. In particular, the Address Count Listing File (<a href="https://www2.census.gov/geo/docs/reference/2020addresscountlist/">link</a>, <a href="https://www.census.gov/geographies/reference-files/2020/geo/2020addcountlisting.html">documentation</a>) which shared invaluable data about the status of the Master Address File (MAF) in the months before the 2020 Census began. This file was the result of an immense undertaking by the USCB and local governments and state demographers to provide updates about housing and GQs across the country to attain complete coverage of the 2020 Census (or as close as possible!). Specifically, it offered counts of housing units and GQ facilities per block-- factors that should be highly correlated with population counts. Could these be used in conjunction with states' own estimates for cities and counties and demographic characteristics from the USCB PEP and ACS programs to produce high quality alternative estimates?

<em>Description:</em><br>
Original report and dataset for Oregon at: https://pdxscholar.library.pdx.edu/prc_pub/45/. This follows nearly identical methodology for Oregon, but applied to the State of California. The only changes were made to use population data from the California Department of Finance (<a href=https://dof.ca.gov/forecasting/demographics/>link</a>) instead of Portland State Univeristy's Population Research Center, and to deal with a few different geography changes between 2010 and 2019 in California compared to Oregon. It can be adapted with small effort to other states, using state population estimates or relying only on data from the USCB PEP. This implementation did not use any proprietary data (the Oregon study used some data from ESRI under license).

Acknowledgements:
I'm grateful to colleagues from the Federal-State Cooperative for Population Estimates, and the fantastic research staff of the US Census Bureau, especially in the Population and Geography Divisions. Without the joint effort of these groups, this work would have been impossible (or would have looked very, very different). 
