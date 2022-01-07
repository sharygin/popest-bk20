2020 Census Block Relationship Data
Voting and Election Science Team
Contact: Brian Amos (brian.amos@wichita.edu)

Release 1.0 (April 10, 2021)

Overview
--------
This dataverse includes crosswalks between 2020 Census block geography and older Census geography, as well as certain data fields reaggregated to the 2020 blocks and rounded to whole numbers. The Census provides crosswalks between 2010 and 2020 block geography (https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.html), but the only measure that is given of what share of a 2010 block belongs to a 2020 block in the case of a split is area. Broadly speaking, the files on this dataverse were made using the method outlined in our (Brian Amos and Michael McDonald) 2017 paper in Public Opinion Quarterly entitled "When Boundaries Collide: Constructing a National Database of Demographic and Voting Statistics," where the National Land Cover Database is used to better predict where people live in the case of a split block.

Files Included
--------------
block10block20_crosswalks
Each row is a 2020 block, with the share of each 2010 block that is assigned to it. A direct application of the POQ paper method, where the share of a split block to assign is determined primarily by the low and medium intesity land cover types in the NLCD.

bg10block20_pop_crosswalks
Each row is a 2020 block, with the share of each 2010 block group that is assigned to it. This is built by taking each 2010 block assignment in the block1020_crosswalks file, multiplying the proportion by the block's population taken from the 2010 Census PL 94-171 Redistrict Data summary file, then dividing by the total block group population of the block.

bg19blocks20_pop_crosswalks
Each row is a 2020 block, with the share of each 2019 block group that is assigned to it. This is built by taking the three-way intersection of the 2010 blocks, the 2020 blocks, and the 2019 block groups shapefiles, then dividing up the 2010 Census PL 94-171 Redistrict Data summary file populations across the 2010 block fragments based on the POQ paper method. These are totaled up by 2019 block group then by 2020 block, with the share of each block group's population being calculated for each intersection with the 2020 block.

bg19blocks20_vap_crosswalks
This is identical to the previous set, but using 2010 Census PL 94-171 Redistrict Data summary file voting-age populations to weight things instead of populations.

census10block20pop
Applies the block10block20_crosswalks to the population from the 2010 Census PL 94-171 Redistrict Data summary file, rounded to whole numbers using the Hamilton/largest remainders apportionment method, which ensures population isn't gained or lost through rounding. The code to do so was Martin Lackner's apportionment (https://github.com/martinlackner/apportionment), with a slight tweak in three lines of the largest_remainder function to allow for non-integer "votes" to be apportioned.

acs19block20pop
Applies the bg19blocks20_pop_crosswalks to the 2015-2019 ACS 5-year total population estimates in the same manner as described for the previous set.

acs19block20cvap
Applies the bg19blocks20_vap_crosswalks to the 2015-2019 ACS 5-year citizen voting-age population estimates in the same manner as described for the previous set.

Sources
-------
Geography:
-Statewide block shapefile releases (both 2010 and 2020) from the Census FTP folder /geo/tiger/TIGER2020PL/LAYER/TABBLOCK downloaded in mid-March 2021. The only alterations were to correct duplicate geography found in the 2010 block shapefiles in California, Kentucky, and Washington.
-Statewide block group shapefiles from the Census FTP folder /geo/tiger/TIGER2019/BG/.
-For land cover data, the NLCD 2016 Land Cover (CONUS) and (ALASKA) files were used from https://www.mrlc.gov/data. The weighting recommended in the POQ paper was used, which primarily focuses on low and medium intensity developed land types. 
-The exception is Hawaii, which is not covered by the NLCD; instead, NOAA's C-CAP High-Resolution data were used (ftp://ftp.coast.noaa.gov/pub/DigitalCoast/raster1/landcover/bulkdownload/hires/hi), the latest release of which was in 2011. The categories of land types are different between the two sources, so the weighting scheme can't be carried over, but testing showed that the "Impervious Surface" type was by far the best predictor of population in a block, so the splits in Hawaii were done fully based on that.

Data:
-2010 Census PL 94-171 Redistrict Data summary file from the Census FTP: /census_2010/01-Redistricting_File--PL_94-171
-2015-2019 American Community Survey 5-year estimates from the Census FTP: /programs-surveys/acs/summary_file/2019/data/5_year_by_state

Use of Crosswalk Files
----------------------
The crosswalk files have the 2020 block GEOID in the first column. The remaining columns alternate between a 2010/2019 GEOID and the share of that geography's population that should be assigned to the 2020 block. For instance, one row in the Arkansas block10block20_crosswalks file is:

050014802001088,050014802002028,1.0,050014802001094,0.15102

This means that to find the value for the 2020 block with GEOID 050014802001088, add the value for the 2010 block with GEOID 050014802002028 and 0.15102 times the value for the 2010 block with GEOID 050014802001094.

Other Notes
-----------
The Census's representation of state boundaries is different in 2010 and 2020 geography, and since the crosswalk was calculated state-by-state, it is possible for blocks in one release to not overlap any blocks in the other release. For the 2010 blocks, these are nearly all zero population, with the exception of geoids 040019440003144, 160099400001100, 481119501002263, and 511410302003031, having populations of 2, 2, 3, and 2, respectively. These were manually assigned to 040019440003011, 160099400001030, 481119501002155, and 511410302012022 in the crosswalk to 2020 instead of making a pairing across state datasets. All 2020 blocks are present in the crosswalks (those with no overlap have no corresponding 2010 block/s given), but zero-population 2010 blocks with no overlap are left out.

Especially small crosswalk values may be expressed in scientific notation (e.g., 7.5e-05).

We provide 2019 crosswalks based on both 2010 population and voting-age population, but in practice, the difference of variables generated by either is small. California 2020 block populations generated by the 2019 ACS using the population crosswalk and the VAP crosswalk correlated at r=.9975.

Disclaimer
----------
We try to make these data as error-free as possible, but we make no guarantees as to the accuracy of the crosswalks or estimated population fields. If you do find a mistake, please don't hesitate to reach out to us about it.