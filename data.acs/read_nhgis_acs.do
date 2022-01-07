qui foreach g in "bg blck_grp" "tr tract" "co county" "pl place" "pu puma" "st state" {
	local a=substr("`g'",1,2)
	local f=substr("`g'",4,.)
	if "`a'"!="tr" & "`a'"!="bg" {
		do nhgis`2'_ds175_2010_`f'.do
		assert _N>0
		keep if statea=="`1'"
		compress
		assert _N>0
		save nhgis_`a'_1acs10.dta, replace
	}
	do nhgis`2'_ds176_20105_`f'.do
	assert _N>0
	keep if statea=="`1'"
	compress
	assert _N>0
	save nhgis_`a'_5acs10.dta, replace
	if "`a'"!="bg" {
		do nhgis`2'_ds177_20105_`f'.do
		assert _N>0
		keep if statea=="`1'"
		if "`a'"=="tr" local key="statea countya tracta"
		else if "`a'"=="pl" local key="statea placea"
		else if "`a'"=="co" local key="statea countya"
		else if "`a'"=="pu" local key="statea puma5a"
		else if "`l'"=="st" local key="statea"
		merge 1:1 `key' using nhgis_`a'_5acs10.dta, assert(3) nogen
		compress
		assert _N>0
		save nhgis_`a'_5acs10.dta, replace
	}
	if "`a'"!="tr" & "`a'"!="bg" {
		do nhgis`2'_ds243_2019_`f'.do
		assert _N>0
		keep if statea=="`1'"
		compress
		assert _N>0
		save nhgis_`a'_1acs19.dta, replace
	}
	do nhgis`2'_ds244_20195_`f'.do
	assert _N>0
	keep if statea=="`1'"
	compress
	assert _N>0
	save nhgis_`a'_5acs19.dta, replace	
	if "`a'"!="bg" {
		do nhgis`2'_ds245_20195_`f'.do
		assert _N>0
		keep if statea=="`1'"
		if "`a'"=="tr" local key="statea countya tracta"
		else if "`a'"=="pl" local key="statea placea"
		else if "`a'"=="co" local key="statea countya"
		else if "`a'"=="pu" local key="statea puma5a"
		else if "`a'"=="st" local key="statea"
		merge 1:1 `key' using nhgis_`a'_5acs19.dta, assert(3) nogen
		compress
		assert _N>0
		save nhgis_`a'_5acs19.dta, replace	
	}
}
