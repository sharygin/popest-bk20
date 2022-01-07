// title: bda_ca
// purpose: research small area population estimates by block:
//	 2020 census blocks with total population + other selected PL94-171 fields
// author: sharygin@pdx.edu/github:sharygin

* TBD:
* use GEO CQR block HU and GQ facility counts?
* add ESRI or HaystaqDNA (redistrictingdatahub.org)
* use additional CA state data
* ask Q ~ is it important that the data come from "before" the census or just best quality alternative?
* add more checks from dec10 and acs19

/* 
history
- v01: first version forked from bda_v15.do (for Oregon). See notes to that file.

methods/outline: streamlined (less controlling in intermediate steps).
* updated blocks: add 2019 hu from luca to update 2010 decennial block data
* uncontrolled/trended blocks: bottom-up trend 2010 pph/race to 2019-20 using esri (block group) + acs (tract) 
* place total control: top-down control of state->county->city/157->trended block result by gq/hh
* puma total control: top-down control of state->county->puma->trended block result by gq/hh
* poststratification/rake for simultaneous fit from trended block to pumas + city/157.
* top-down control total by race from state total->state acs->county acs->puma acs, if applicable->trended blocks.
* geographic reallocation to 2020 blocks.
* rounding and fixing results to be consistent with controlled state totals.
* rounding and fixing results by race to be consistent with the rounded, fixed block totals.
tbd: simplifying reweighting procedures, after rounding no need to control to any total except:
	(1) total population by block to statewide.
		population by race + block to total by block.

tbd: consider alternative method: race first.
* updated blocks: add 2019 hu from luca to update 2010 decennial block dta (total hh pop by block)
* uncontrolled trended blocks: botom up trend 2010 pph/race to 2019-20 esri BG + 2015-19 acs tracts (when significantly different and nonzero)
* place total control: top-down control of state->county->city/157->trended block result by gq/hh
* puma total control: top-down control of state->county->puma->trended block result by gq/hh
* control updated trended blocks by race to sum to overall totals by controlled city/157.
* control updated trended blocks by race to sum to totals by race by controlled PUMA.
* geographic reallocation to 2020 blocks.
* final rounding and fixing results to be consistent with controlled state totals by race.
tbd: how to handle GQs well in this approach. 
* updated GQ counts: ACS PUMS tot by race state -> PEP tot by county -> ESRI tot by BG
* race w/in updated GQs: use to remove GQ pop by race from the total pop = hhpop?
	don't need race by gq, but should use to remove some population by race from the total.
	dec10 PCT20 A-I gqtype w/races: A-I (wa,ba,na,aa,pa,oa,mr,h,nhw)
	acs19 puma or county level breakdown by races, to separate the above.
	express as a probability weight, and select out of the small area population.
	
*/

// setup and path
	global path "D:/users/iisan7/pdx/PROJECTS/_misc/103_RAND_cabda_chpse_redux" // path to project folder
	foreach pkg in "bacon st0197.pkg" "survwgt survwgt" "filelist filelist" {
		tokenize `pkg' 
		cap which `1' // check required package
		if _rc==111 net install `2' // install by pkg name
	}
	cap mkdir "$path/data.working/"
	
// filenames
	local nhgisdec "nhgis0065_fixed" // nhgis file with 2010 SF1 tables for blocks: P1, P5, P42, H3.
	local nhgisacs "nhgis0066_fixed" // nhgis file with ACS SF tables for blockgroups: b26001, b25002, b03002, b09019.

/************************************
DEC BASELINE
purpose: save starting population by block of: pop by race, tothu, gqpop/hhpop + hispanic races
************************************/

// 2010 Tract to PUMA crosswalk
	import delim using "$path/data.census/2010_Census_Tract_to_2010_PUMA.txt", clear stringc(1 2 3 4) case(lower)
	keep if statefp=="06" // keep CA only
	gen geopu="97100US06"+puma5ce
	gen geotr="14000US06"+countyfp+tractce
	contract geopu geotr
	subsave geopu geotr using "$path/data.census/tr_to_puma.dta", replace

// DEC BK/BG/TR: final tables P1, P5, P42, H3 
	* totpop: _p1_ totpop 
	* gqpop: _p42_ gqpop by gqtype 
	* vacancy/pph: _h3_ occupancy status
	* race: _p5_ race by hisp (12) or _p9_ or _p11_ hisp by race by lt/ge 18yo (70)
	** save race distributions of hispanic population for geographic levels: state->county->puma->tract
	** apply to ACS controls to create a hybrid: ACS for non-hispanic by race and hispanic total; DEC10 for hisp by race.
	** reason is that ACS race by hispanic is erratic, even in 2010 shows 65% white, whereas DEC10 is 45%.
	cap confirm file "$path/data.census/nhgis_bk_dec10.dta" 
	if _rc {
		unzipfile "$path/data.census/nhgis0065_fixed.zip"
		cd "$path/data.census/nhgis0065_fixed" // ipums dofiles must be run in working directory
		do "nhgis0065_ds172_2010_block.do" // read raw text fwf
		compress
		cd "$path/"
		do "$path/data.census/rename_nhgis_dec.do" // consistent names for ipums vars from P1, P5, P42, H3, P9, P11
		save "$path/data.census/nhgis_bk_dec10.dta", replace
		shell rmdir "nhgis0065_fixed" /s /q // erase unzipped files w/o confirmation (careful!)
	}
	cap confirm file "$path/data.working/dec_st_hisprace.dta" // check if done
	if _rc { 
		use "$path/data.census/nhgis_bk_dec10.dta", clear
		#delimit ;
		ren p01001 totpop;	ren p42001 gqpop;  ren h03001 tothu;
		ren p05001 rtotpop; ren p05002 rtotnh; ren p05010 rtoth;
		ren p05003 wnh; ren p05004 bnh; ren p05005 nnh; ren p05006 anh; ren p05007 pnh; ren p05008 onh; ren p05009 mnh;
		ren p05011 wh; ren p05012 bh; ren p05013 nh; ren p05014 ah; ren p05015 ph; ren p05016 oh; ren p05017 mh;
		#delimit cr
		gen hhpop=totpop-gqpop
		gen hpph=hhpop/tothu
		gen geobk="10100US"+statea+countya+tracta+blocka // block geoid=101
		gen geobg="15000US"+statea+countya+tracta+blkgrpa // block group geoid=150
		gen geotr="14000US"+statea+countya+tracta
		keep geobk geobg geotr totpop hhpop gqpop tothu hpph rtot* w*h b*h n*h a*h p*h o*h m*h
		** dec bk
		order geobk geobg geotr
		for any "w" "b" "n" "a" "p" "o" "m": gen Xnh_shr=Xnh/rtotpop \\ gen Xh_shr=Xh/rtotpop
		save "$path/data.working/dec_bk.dta", replace
		** dec bg
		gcollapse (sum) rtot* w*h b*h n*h a*h p*h o*h m*h tothu gqpop hhpop totpop, by(geobg geotr)
		gen hpph=hhpop/tothu
		keep geobg geotr totpop hhpop gqpop tothu hpph rtot* w*h b*h n*h a*h p*h o*h m*h
		for any "w" "b" "n" "a" "p" "o" "m": gen Xnh_shr=Xnh/rtotpop \\ gen Xh_shr=Xh/rtotpop
		save "$path/data.working/dec_bg.dta", replace
		for var wh bh nh ah ph oh mh: gen X_shrh=X/rtoth
		subsave geobg *shrh using "$path/data.working/dec_bg_hisprace.dta", replace
		** dec tr
		drop *shrh
		gcollapse (sum) rtot* w*h b*h n*h a*h p*h o*h m*h tothu gqpop hhpop totpop, by(geotr)
		gen hpph=hhpop/tothu
		keep geotr totpop hhpop gqpop tothu hpph rtot* w*h b*h n*h a*h p*h o*h m*h
		for any "w" "b" "n" "a" "p" "o" "m": gen Xnh_shr=Xnh/rtotpop \\ gen Xh_shr=Xh/rtotpop
		save "$path/data.working/dec_tr.dta", replace // not currently used
		for var wh bh nh ah ph oh mh: gen X_shrh=X/rtoth
		subsave geotr *shrh using "$path/data.working/dec_tr_hisprace.dta", replace
		** dec pu
		drop *shrh
		preserve
		merge m:1 geotr using "$path/data.census/tr_to_puma.dta", assert(3) keepus(geopu) nogen
		gcollapse (sum) rtot* w*h b*h n*h a*h p*h o*h m*h tothu gqpop hhpop totpop, by(geopu)
		for var wh bh nh ah ph oh mh: gen X_shrh=X/rtoth
		subsave geopu *shrh using "$path/data.working/dec_pu_hisprace.dta", replace
		restore
		** dec co 
		gen geoco="05000US"+substr(geotr,8,5)
		gcollapse (sum) rtot* w*h b*h n*h a*h p*h o*h m*h tothu gqpop hhpop totpop, by(geoco)
		for var wh bh nh ah ph oh mh: gen X_shrh=X/rtoth
		subsave geoco *shrh using "$path/data.working/dec_co_hisprace.dta", replace
		** dec st
		drop *shrh
		gen geost="04000US"+statea
		gcollapse (sum) rtot* w*h b*h n*h a*h p*h o*h m*h tothu gqpop hhpop totpop, by(geost)
		for var wh bh nh ah ph oh mh: gen X_shrh=X/rtoth
		subsave geost *shrh using "$path/data.working/dec_st_hisprace.dta", replace
	} // end condition

/************************************
ACS CHANGES
purpose: save changes by blockgroup in housing units, household size, GQ population changes
************************************/

// Generate datasets by geosumlev/years from NHGIS extract
	** tables: b26001, b25002, b03002, b09019 = ds176 ds177 ds243 ds245 in ipums names
	** geosumlevs:  bk 101, bg 105, trt 140, ccd 060, puma 795, city 157, county 050, state 040
	** years: 2010, 2006-10, 2019, 2015-19
	cap confirm file "$path/data.acs/nhgis_st_5acs19.dta" // check if this example file is present
	if _rc {
		cd $path/data.acs/
		unzipfile nhgis0066_fixed.zip
		cd $path/data.acs/nhgis0066_fixed/
		do ../read_nhgis_acs.do 06 0066 // pass arguments to the dofile for state fips code + NHGIS request #
		shell move *.dta ../
		shell move *.txt ../
		cd $path/data.acs/
		shell rmdir "nhgis0066_fixed" /s /q // erase unzipped files w/o confirmation (careful!)
	}
	**
	**
	** HOUSING UNITS CHANGE
	** purpose: update hpph and tothu (by blockgroup trends)
	// acs 2015-19 pph at the BG level
	use "$path/data.acs/nhgis_bg_5acs19.dta", clear
	#delimit ;
	ren alu6e001 totpop; ren alu6m001 totpopm;
	ren alu6e002 hhpop; ren alu6m002 hhpopm;
	ren alu6e026 gqpop; ren alu6m026 gqpopm;
	ren alzke001 tothu; ren alzkm001 tothum;
	ren alzke003 vachu; ren alzkm003 vachum;
	#delimit cr
	gen hpph=hhpop/tothu
	gen hpphm=(1/tothum)*sqrt(hhpopm^2-(hhpop/tothu)^2*tothum) // check this
	gen geobg="15000US"+statea+countya+tracta+blkgrpa
	save "$path/data.working/acs19_bg_hpph.dta", replace		
	**
	** measure changes
	// dec-acs test for significant changes in hpph at the BG level
	** merge dec with acs1519
	use geobg hhpop hpph using "$path/data.working/dec_bg.dta", clear
	ren hhpop hhpop10
	ren hpph hpph10
	merge 1:1 geobg using "$path/data.working/acs19_bg_hpph.dta", keepus(hhpop hpph hpphm) // assert(3) nogen
	cap assert _merge==3 
	if _rc {
		list if _merge!=3 // for CA, see: https://www2.census.gov/geo/pdfs/reference/Geography_Notes.pdf
		** CA specific adjustments ~ adjust blocks.
		** this should be done correctly at the block level following the geography note.
		** however, I'm going to dummy it by keeping both blocks and assigning NO CHANGE.
		replace hhpop10=hhpop if _merge==2
		replace hpph10=hpph if _merge==2
		replace hhpop=hhpop10 if _merge==1
		replace hpph=hpph10 if _merge==1
		replace hpphm=0 if _merge==1
	}
	ren hhpop hhpop19 
	ren hpph hpph19
	ren hpphm hpph19m
	gen hpph19se=hpph19m/1.645
	** test for significant difference
	gen diff=hpph19-hpph10
	gen sqse2=sqrt(0^2+hpph19se^2)
	gen chk=(abs(diff/sqse2)>1.645) if hpph19<. & hpph19>0 // 95% confident difference (ACS handbook #7)
	tab chk // ~ 12% or 322 of 2,623 BGs had significant differences from 2010
	** apply changes 
	replace hpph10=hpph19 if hhpop10==0 & hhpop19>0 & hhpop19<.
	gen ratio=hpph19/hpph10 
	replace ratio=1.0 if (hpph19==. | hpph19==0) // fix missings
	replace ratio=1.0 if chk!=1 // remove insignificant changes
	assert ratio<. 
	table geobg if chk==1, contents(n chk mean hhpop10 mean hhpop19 mean ratio)
	replace ratio=1.0 // virtually none can be confirmed different from 2010 = don't change?
	// !!! there is a deep problem with the bg ratio applied to block paradigm.
	keep geobg ratio hpph10 hpph19 // BG geoid and ratio
	ren hpph19 hpph_acs19bg
	save "$path/data.working/dec_ratio_hpph_acs.dta", replace // not very useful ~ acs noise
	**
	// apply changes
	** assume change in all component blocks of BG
	use "$path/data.working/dec_bk.dta", clear
	ren hpph hpph10 
	merge m:1 geobg using "$path/data.working/dec_ratio_hpph_acs.dta" // , assert(3) nogen
	list geo* if _merge!=3
	keep if _merge==3
	drop _merge
	ren ratio ratio_acs
	gen hpph19=hpph10*ratio_acs // 2019 hpph 
	* test if an outlier
	* scatter hpph10 hpph19 if hpph10<=15 & hpph19<=15, aspect(1)
	bacon hpph19, gen(flag) p(.01) // topcoded at 8.5pph
	table flag, contents(count hhpop  sum hhpop sum tothu mean hpph10 mean hpph19) row
	* outliers may be legitimate; unedited from DEC10
	* next, add BG hpph if BK is missing, so that all BK have a theoretical HPPH
	* this is useful in case LUCA19 has >0 HU but DEC10 had 0 HU so no HPPH.
	replace hpph19=hpph_acs19bg if hpph19==. & hpph_acs19bg<. 
	replace hpph19=hpph10 if hpph19==. & hpph10<. 
	save "$path/data.working/dec_ratio_hpph.dta", replace 
	rm "$path/data.working/dec_ratio_hpph_acs.dta"
	**
	**
	** GROUP QUARTERS CHANGE
	** purpose: update gqpop (from trends by tract)
	// ACS BG GQ pop
	** 2015-19 only
	use "$path/data.acs/nhgis_bg_5acs19.dta", clear
	#delimit ;
	ren alu6e001 totpop; ren alu6m001 totpopm;
	ren alu6e002 hhpop; ren alu6m002 hhpopm;
	ren alu6e026 gqpop; ren alu6m026 gqpopm;
	ren alzke001 tothu; ren alzkm001 tothum;
	ren alzke003 vachu; ren alzkm003 vachum;
	#delimit cr
	gen hpph=hhpop/tothu
	gen hpphm=(1/tothum)*sqrt(hhpopm^2-(hhpop/tothu)^2*tothum) // check this
	gen geobg="15000US"+statea+countya+tracta+blkgrpa
	save "$path/data.working/acs19_bg_gq.dta", replace
	// ACS tract GQ pop
	** 2015-19 only
	use "$path/data.acs/nhgis_tr_5acs19.dta", clear
	#delimit ;
	ren amj0e001 gqpop; ren amj0m001 gqpopm;
	ren alzke001 tothu;	ren alzkm001 tothum;
	ren alzke003 vachu;	ren alzkm003 vachum;
	ren alu6e001 totpop; ren alu6m001 totpopm;
	ren alu6e002 hhpop; ren alu6m002 hhpopm;
	#delimit cr
	gen hpph=hhpop/tothu
	gen hpphm=(1/tothum)*sqrt(hhpopm^2-(hhpop/tothu)^2*tothum) // check this
	gen geotr="14000US"+statea+countya+tracta
	order geotr 
	keep geotr gqpop gqpopm totpop totpopm hhpop hhpopm hpph hpphm
	save "$path/data.working/acs19_tr_gq.dta", replace
	//
	// DEC-ACS test for significant non-zero differences @ tract level GQpop 
	use "$path/data.working/dec_bg.dta", clear
	collapse (sum) gqpop, by(geotr)
	gen int year=2010
	ren gqpop gqpop10
	merge 1:1 geotr using "$path/data.working/acs19_tr_gq.dta", keepus(gqpop gqpopm) // assert(3) 
	cap assert _merge==3 
	if _rc {
		list if _merge!=3 // for CA, see: https://www2.census.gov/geo/pdfs/reference/Geography_Notes.pdf
		** CA specific adjustments ~ adjust blocks.
		** this should be done correctly at the block level following the geography note.
		** however, I'm going to dummy it by keeping both blocks and assigning NO CHANGE.
		replace gqpop10=gqpop if _merge==2
		replace gqpop=gqpop10 if _merge==1
		replace gqpopm=0 if _merge==1
	}
	ren gqpop gqpop19
	ren gqpopm gqpop19m
	gen gqpop19se=gqpop19m/1.645
	gen diff=gqpop19-gqpop10
	gen sqse2=sqrt(0+gqpop19se^2)
	** chk for significant difference
	gen chk=.
	local tcrit=2.58 // 19.6 95% 2.58 99%
	replace chk=(abs(diff/sqse2)>`tcrit') if gqpop19>0 & gqpop19<. // 95/99% confident difference (ACS handbook #7)
	tab chk // ~ 92 of 696 or 13% tracts had significant differences from 2010 @ 95%; 60 or 8.6% @ 99%
	** ratio for block extrapolation
	gen ratio=1.0 // keep constant if no significant difference
	replace ratio=gqpop19/gqpop10 if chk & gqpop19/gqpop10<. & gqpop19>0 // reject ACS zeros
	assert ratio<. 
	** no outlier checks; assume GQ counts in 2019 are accurate if they are different.
	keep geotr gqpop10 gqpop19 gqpop19m ratio
	* tw scatter gqpop10 gqpop19 [fw=gqpop10], aspect(1) xsize(3) ysize(3)
	* table ratio, contents(n gqpop10 mean gqpop10 mean gqpop19)
	save "$path/data.working/dec_ratio_gq_acs.dta" 
	//
	// store DEC update ratios
	** calculate ratios of change for uncontrolled block extrapolations.
	** eventually, will control GQ population at the place and county levels.
	use "$path/data.working/dec_bk.dta", clear
	ren gqpop gqpop10
	merge m:1 geotr using "$path/data.working/dec_ratio_gq_acs.dta", keep(1 3) nogen
	ren ratio ratio_acs
	renpfix gqpop19 gqpop_acs
	gen gqpopa=gqpop10*ratio_acs // 2019 hpph based on ACS trt
	* examine
	* scatter gqpop10 gqpopa [fw=gqpop10], aspect(1)
	tabstat gqpop10 gqpopa, stats(sum)
	save "$path/data.working/dec_ratio_gq.dta", replace 
	rm "$path/data.working/dec_ratio_gq_acs.dta"

/************************************
DEC UPDATE FROM ACS TRENDS (UNCONTROLLED)
purpose: generate an extrapolated population reflecting indications of change since 2010
************************************/

// calculate new total population from DEC-ACS PPH * LUCA19 HUs
	// HHPOP from luca19 count of housing units + gq facilities per block
	** a block suffix BLKSF indicates a 2010 block that was split over the course of the decade.
	** strip off block suffix (BLKSF_2010) and collapse sum. 
	** it would theoretically increase accuracy to consider these when doing the 2010:2020 split.
	import delim "$path/data.geo/06_California_AddressBlockCountList_012020.txt", delim("|") clear
	keep if state==6 // drop other states intersecting peripheral blocks
	ren totalresidential tothu19
	ren totalgroupquarters totgq19 // N of GQ facilities -- no analogue in 2010 or in ACS (those give GQ population only)
	gen geobk="10100US"+string(state,"%02.0f")+string(county,"%03.0f")+string(tract*100,"%06.0f")+substr(block,1,4) // strip BLKSF from block
	gcollapse (sum) tothu19 totgq19, by(geobk) // collapse to original 2010 blocks
	gen geobg="15000US"+substr(geobk,8,12)
	save "$path/data.working/luca_bk.dta", replace
	use "$path/data.working/luca_bk.dta" 
	merge 1:1 geobk using "$path/data.working/dec_ratio_hpph.dta" 
	table _merge, contents(sum totpop)
	assert totpop==0 if _merge!=3
	drop _merge
	for var tothu19 totgq19: replace X=0 if X==.
	** hpph19 uses trended block hpph, or ~2019 block group if 2010 pph missing/zero.
	gen hhpop19=hpph19*tothu19 // luca 2019 units + new 2019 hpph
	replace hhpop19=0 if tothu19==0 
	replace hhpop19=0 if hhpop19==. & tothu19>0 & tothu19<.
	replace hhpop=0 if hhpop==. & tothu>0 & tothu<.
	assert hhpop19==0 if tothu19==0 
	assert (hhpop19>=0 & hhpop19<.) if (tothu19<. & tothu19>0) // if there are houses in 2019, were there 0+ people?
	assert (hhpop19>0 & hhpop19<.) if (tothu19<. & tothu19>0) & (hpph19>0 & hpph19<.)
	// cleanup
	keep geobk geobg tothu19 totgq19 hhpop19 
	gen hpph19=hhpop19/tothu19
	replace hpph19=0 if hpph19==. // replace missing with Zero.
	save "$path/bda_bk1.dta", replace // new working file: hhpop19 tothu19 totgq19 
	
// calculate new GQ population from base/extrapolation + LUCA19 facilities
	// GQPOP from extrapolation of tract-level trends
	use "$path/bda_bk1.dta", clear
	merge 1:1 geobk using "$path/data.working/dec_ratio_gq.dta", keep(1 3) keepus(*gq*) 
	** add dummy pop to GQs not in 2010
	unique geobk if gqpop10==0 & (totgq19>0 & totgq19<.) // N=560 blocks where there were 0 GQpop in 2010 and >0facil in 2019
	qui sum totgq19 if gqpop10==0 & (totgq19>0 & totgq19<.) 
	nois di "`r(sum)'" // N=901 facilities 
	** new extrapolated block GQ population (gqpopa=ACS gqpope=ESRI gqpop10=DEC)
	gen gqpop19=gqpop10 // choice of base GQ population to use
	replace gqpop19=0 if totgq19==0 // remove GQpop if noGQs anymore
	replace gqpop19=totgq19*1 if (gqpop19==0|gqpop19==.) & (totgq19>=2 & totgq19<.) // seed dummy pop == X*N of missed GQ. 
	// cleanup
	keep geobk geobg totgq19 gqpop19 tothu19 hhpop19 hpph19 
	save "$path/bda_bk2.dta", replace

// calculate new population by race 
	// proxy distribution, shares by bg/tract, for imputing if no dec_bk population
	// add race to baseline totpop
	use "$path/bda_bk2.dta", clear
	for var gqpop19 hhpop19: replace X=0 if X==.
	gen totpop=gqpop19+hhpop19
	assert totpop<.
	merge 1:1 geobk using "$path/data.working/dec_bk.dta", assert(3) keepus(*shr) nogen // dec10 blocks
	foreach r in "w" "b" "n" "a" "p" "o" "m" {
		gen `r'nh=`r'nh_shr*totpop 
		replace `r'nh=0 if `r'nh==. 
		gen `r'h=`r'h_shr*totpop
		replace `r'h=0 if `r'h==. 
	}
	drop *shr
	merge m:1 geobg using "$path/data.working/dec_bg.dta", assert(3) keepus(*shr) nogen // dec10 bgs
	foreach r in "w" "b" "n" "a" "p" "o" "m" {
		replace `r'nh=`r'nh_shr*totpop if `r'nh==0 & `r'nh_shr<.
		replace `r'nh=0 if `r'nh==. 
		replace `r'h=`r'h_shr*totpop if `r'h==0 & `r'h_shr<.
		replace `r'h=0 if `r'h==. 
	}
	drop *shr
	gen geotr="14000US"+substr(geobk,8,11)
	merge m:1 geotr using "$path/data.working/dec_tr.dta", assert(3) keepus(*shr) nogen // dec10 trs
	foreach r in "w" "b" "n" "a" "p" "o" "m" {
		replace `r'nh=`r'nh_shr*totpop if `r'nh==0 & `r'nh_shr<.
		replace `r'nh=0 if `r'nh==. 
		replace `r'h=`r'h_shr*totpop if `r'h==0 & `r'h_shr<.
		replace `r'h=0 if `r'h==. 
	}
	drop *shr
	// check that every block has something, either from 2010 bk/bg/ or tr.
	egen chk=rowtotal(wnh-mh)
	assert chk>0 if hhpop19>0 
	assert hhpop19==0 if hpph19==0 | tothu19==0
	assert hhpop19>0 if tothu19>0 & hpph19>0
	drop chk
	save "$path/bda_bk3.dta", replace 
	
/************************************
PREPARE CONTROL TOTALS 
purpose: decide which top-level population counts will be imposed as external control totals
************************************/

// DEC20 state resident 2020 population from official release
// PRC state, county, and place totals
// PEP state, county, and place totals
// ACS state, PUMA, county, and tract distributions by race/ethnicity (all 5yr)
// DEC10 breakdown of hispanic by race (used to adjust ACS hispanic race distributions from ACS)

// (sumlev 40) store apportionment total (APR1)
** Calif 2020 apport: 4,241,500 resident: 4,237,256 overseas: 4,244	
** Calif 2010 apport: 3,848,606 resident: 3,831,074 overseas: 17,532
	import excel using "$path/data.census/apportionment-2020-tableA.xlsx", clear cellrange(a10:g10) 
	gen geost="04000US06"
	ren A stname
	ren B apportionment2020
	ren C resident2020
	ren D overseas2020
	ren E apportionment2010
	ren F resident2010
	ren G overseas2010
	subsave geost resident2020 using "$path/data.working/stdec20.dta", replace

// (sumlev 157) dru place/county/state (JAN1)
	** DRU: city and county totals (https://dof.ca.gov/forecasting/demographics/Estimates/)
	** custom modified to add geoid matching census definitions for sumlev 157/40/50
	import delim using "$path/data.dru/E-5-2021_by_Geo_Recoded.csv", clear
	destring *, replace ignore(",%")
	replace vacrate=vacrate/100
	gen sumlev=real(substr(geoid,1,3))
	gen geoco=geoid if sumlev==50
	gen geopl=geoid if sumlev==157
	gen geost=geoid if sumlev==40
	keep if mdy=="1/1/2020" // keep only 2020
	rename totpop druest20
	rename gqpop drugq20
	subsave geopl drugq20 druest20 if sumlev==157 using "$path/data.working/pldru20.dta", replace
	subsave geoco drugq20 druest20 if sumlev==50 using "$path/data.working/codru20.dta", replace 
	subsave geost drugq20 druest20 if sumlev==40 using "$path/data.working/stdru20.dta", replace 
	*mkmat totpop if substr(geoid,1,3)=="157", rownames(geoid) matrix(pldru20)
	*mkmat totpop if substr(geoid,1,3)=="050", rownames(geoid) matrix(codru20)

// (sumlev 50) pep place total/county hh-gq/state
	** SUMLEVS 157: county-incorporated place (parts) 155 = same but includes CDPs
	** 		   160 = place within state 162 = incorporated place w/in state
	** PEP vtg20 subco (includes sumlev 40 50 157 162) 
	import delim using "$path/data.pep/sub-est2020_6.csv", clear
	gen geopl=string(sumlev,"%03.0f")+"00US"+string(state,"%02.0f")+string(county,"%03.0f")+string(place,"%05.0f") if sumlev==157
	ren popestimate2020 pepest2020
	subsave geopl pepest2020 if sumlev==157 using "$path/data.working/plpep20.dta", replace
	* mkmat pepest2020 if sumlev==157, rownames(geopl) matrix(plpep20)
	** PEP vtg20 co (sumlev 50) 
	import delim using "$path/data.pep/co-est2020-alldata.csv", clear
	gen geoco=string(sumlev,"%03.0f")+"00US"+string(state,"%02.0f")+string(county,"%03.0f") if sumlev==50
	ren popestimate2020 pepest2020
	ren gqestimates2020 pepgq2020
	subsave geoco pepest2020 pepgq2020 if sumlev==50 & state==6 using "$path/data.working/copep20.dta", replace
	* mkmat pepest2020 gqestimates2020 if state==6 & sumlev==50, rownames(geoco) matrix(copep20)
	** PEP vtg20 st by 18yo (sumlev 40)
	* import delim using "$path/data.pep/sc-est2020-18+pop-res.csv", clear
	* gen geost=string(sumlev,"%03.0f")+"00US"+string(state,"%02.0f") if sumlev==40
	
// (sumlev 140 971 50 40) acs tract/puma/county/state totals by race/ethnicity
// ACS demographic controls
	// 5ACS19 tracts
	use if statea=="06" using "$path/data.acs/nhgis_tr_5acs19.dta", clear
	ren geoid geotr
	# delimit ;
	ren aluke001 totpop	;	ren alukm001 totpop_moe	;
	ren aluke002 totnh	;	ren alukm002 totnh_moe  ;
	ren aluke003 wnh	;	ren alukm003 wnh_moe    ;
	ren aluke004 bnh	;   ren alukm004 bnh_moe    ;
	ren aluke005 nnh	;   ren alukm005 nnh_moe    ;
	ren aluke006 anh	;   ren alukm006 anh_moe    ;
	ren aluke007 pnh	;   ren alukm007 pnh_moe    ;
	ren aluke008 onh	;   ren alukm008 onh_moe    ;
	ren aluke009 mnh	;   ren alukm009 mnh_moe    ;
	ren aluke012 toth	;   ren alukm012 toth_moe   ;
	ren aluke013 wh		;	ren alukm013 wh_moe     ;
	ren aluke014 bh		;   ren alukm014 bh_moe     ;
	ren aluke015 nh		;   ren alukm015 nh_moe     ;
	ren aluke016 ah		;   ren alukm016 ah_moe     ;
	ren aluke017 ph		;   ren alukm017 ph_moe     ;
	ren aluke018 oh		;   ren alukm018 oh_moe     ;
	ren aluke019 mh		;   ren alukm019 mh_moe     ;
	ren alu6e002 hhpop	; 	ren alu6m002 hhpop_moe	;
	ren alu6e026 gqpop	;	ren alu6m026 gqpop_moe	;
	#delimit cr
	keep geotr tot* w*h b*h n*h a*h p*h o*h m*h hhpop* gqpop*
	** replace hispanic by race with DEC10 based shares
	replace geotr="14000US06037930401" if geotr=="14000US06037137000"
	merge 1:1 geotr using "$path/data.working/dec_tr_hisprace.dta", keepus(*shrh) update assert(3 4 5) nogen 
	gen geoco="05000US"+substr(geotr,8,5)
	merge m:1 geoco using "$path/data.working/dec_co_hisprace.dta", assert(3 4 5) keepus(*shrh) nogen update // tracts missing hispanic pop in 2010
	for any "w" "b" "n" "a" "p" "o" "m": assert Xh_shrh<. \\ gen Xh_dec=round(toth*Xh_shrh)
	egen chk=rowtotal(wh_dec bh_dec nh_dec ah_dec ph_dec oh_dec mh_dec) 
	gen diff=toth-chk
	sum diff if diff!=0
	while `r(N)'>0 { 
		gen draw=runiform() if diff!=0 
		replace wh_dec=wh_dec+1 if inrange(draw,0,wh_shrh) & diff>0
		replace bh_dec=bh_dec+1 if inrange(draw,wh_shrh,wh_shrh+bh_shrh) & diff>0
		replace nh_dec=nh_dec+1 if inrange(draw,wh_shrh+bh_shrh,wh_shrh+bh_shrh+nh_shrh) & diff>0
		replace ah_dec=ah_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh) & diff>0
		replace ph_dec=ph_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh) & diff>0
		replace oh_dec=oh_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh) & diff>0
		replace mh_dec=mh_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh+mh_shrh) & diff>0
		replace wh_dec=wh_dec-1 if inrange(draw,0,wh_shrh) & diff<0
		replace bh_dec=bh_dec-1 if inrange(draw,wh_shrh,wh_shrh+bh_shrh) & diff<0
		replace nh_dec=nh_dec-1 if inrange(draw,wh_shrh+bh_shrh,wh_shrh+bh_shrh+nh_shrh) & diff<0
		replace ah_dec=ah_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh) & diff<0
		replace ph_dec=ph_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh) & diff<0
		replace oh_dec=oh_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh) & diff<0
		replace mh_dec=mh_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh+mh_shrh) & diff<0
		drop chk diff draw
		egen chk=rowtotal(wh_dec bh_dec nh_dec ah_dec ph_dec oh_dec mh_dec) 
		gen diff=toth-chk
		sum diff if diff!=0		
	}
	drop chk diff
	save "$path/data.working/acs19_tract.dta", replace // tract
	// 5ACS19 puma pop by race for counties with >1 puma
	/*  01 Alameda 		00101,00110
		07 Butte		00701,00702
		13 Contra Costa	01301,01309
		19 Fresno		01901,01907
		29 Kern			02901,02905
		37 Los Angeles	03701,03769
		41 Marin		04101,04102
		47 Merced		04701,04702
		53 Monterey		05301,05303
		59 Orange		05901,05918
		61 Placer		06101,06103
		65 Riverside	06501,06515
		67 Sacramento	06701,06712
		71 San Bernard. 07101,07115
		73 San Diego	07301,07322
		75 San Francis. 07501,07507
		77 San Joaquin 	07701,07704
		79 San Luis Ob. 07901,07902
		81 San Mateo	08101,08106
		83 Santa Barba. 08301,08303
		85 Santa Clara	08501,08514
		87 Santa Cruz	08701,08702
		95 Solano		09501,09503
		97 Sonoma		09701,09703
		99 Stanislaus	09901,09904
		107 Tulare		10701,10703
		111 Ventura		11101,11106 */
	use if inrange(puma5a,"00101","00110") | inrange(puma5a,"00701","00702") | inrange(puma5a,"01301","01309") | ///
		   inrange(puma5a,"01901","01907") | inrange(puma5a,"02901","02905") | inrange(puma5a,"03701","03769") | ///
		   inrange(puma5a,"04101","04102") | inrange(puma5a,"04701","04702") | inrange(puma5a,"05301","05303") | ///
		   inrange(puma5a,"05901","05918") | inrange(puma5a,"06101","06103") | inrange(puma5a,"06501","06515") | ///
		   inrange(puma5a,"06701","06712") | inrange(puma5a,"07101","07115") | inrange(puma5a,"07301","07322") | ///
		   inrange(puma5a,"07501","07507") | inrange(puma5a,"07701","07704") | inrange(puma5a,"07901","07902") | ///
		   inrange(puma5a,"08101","08106") | inrange(puma5a,"08301","08303") | inrange(puma5a,"08501","08514") | ///
		   inrange(puma5a,"08701","08702") | inrange(puma5a,"09501","09503") | inrange(puma5a,"09701","09703") | ///
		   inrange(puma5a,"09901","09904") | inrange(puma5a,"10701","10703") | inrange(puma5a,"11101","11106") ///
		using "$path/data.acs/nhgis_pu_5acs19.dta", clear
	gen geopu="97100US"+statea+puma5a
	#delimit ;
	ren aluke001 rtotpop; 
	ren aluke002 rtotnh; ren aluke003 wnh; ren aluke004 bnh; ren aluke005 nnh; 
	ren aluke006 anh; ren aluke007 pnh; ren aluke008 onh; ren aluke009 mnh;
    ren aluke012 rtoth; ren aluke013 wh; ren aluke014 bh; ren aluke015 nh;
    ren aluke016 ah; ren aluke017 ph; ren aluke018 oh; ren aluke019 mh;
	ren amj0e001 gqpop; ren alu6e002 hhpop;
	#delimit cr
	drop aluke*
	keep geopu gqpop hhpop rtot* w*h b*h n*h a*h p*h o*h m*h
	** replace hispanic by race with DEC10 based shares
	merge 1:1 geopu using "$path/data.working/dec_pu_hisprace.dta", assert(2 3 4 5) keep(3 4 5) keepus(*shrh) nogen update // keep 3+ because pu_hisprace contains more PUMAs than used in this analysis.
	gen geoco="05000US"
	replace geoco=geoco+"06001" if inrange(substr(geopu,-5,.),"00101","00110")
	replace geoco=geoco+"06007" if inrange(substr(geopu,-5,.),"00701","00702")
	replace geoco=geoco+"06013" if inrange(substr(geopu,-5,.),"01301","01309")
	replace geoco=geoco+"06019" if inrange(substr(geopu,-5,.),"01901","01907")
	replace geoco=geoco+"06029" if inrange(substr(geopu,-5,.),"02901","02905")
	replace geoco=geoco+"06037" if inrange(substr(geopu,-5,.),"03701","03769")
	replace geoco=geoco+"06041" if inrange(substr(geopu,-5,.),"04101","04102")
	replace geoco=geoco+"06047" if inrange(substr(geopu,-5,.),"04701","04702")
	replace geoco=geoco+"06053" if inrange(substr(geopu,-5,.),"05301","05303")
	replace geoco=geoco+"06059" if inrange(substr(geopu,-5,.),"05901","05918")
	replace geoco=geoco+"06061" if inrange(substr(geopu,-5,.),"06101","06103")
	replace geoco=geoco+"06065" if inrange(substr(geopu,-5,.),"06501","06515")
	replace geoco=geoco+"06067" if inrange(substr(geopu,-5,.),"06701","06712")
	replace geoco=geoco+"06071" if inrange(substr(geopu,-5,.),"07101","07115")
	replace geoco=geoco+"06073" if inrange(substr(geopu,-5,.),"07301","07322")
	replace geoco=geoco+"06075" if inrange(substr(geopu,-5,.),"07501","07507")
	replace geoco=geoco+"06077" if inrange(substr(geopu,-5,.),"07701","07704")
	replace geoco=geoco+"06079" if inrange(substr(geopu,-5,.),"07901","07902")
	replace geoco=geoco+"06081" if inrange(substr(geopu,-5,.),"08101","08106")
	replace geoco=geoco+"06083" if inrange(substr(geopu,-5,.),"08301","08303")
	replace geoco=geoco+"06085" if inrange(substr(geopu,-5,.),"08501","08514")
	replace geoco=geoco+"06087" if inrange(substr(geopu,-5,.),"08701","08702")
	replace geoco=geoco+"06095" if inrange(substr(geopu,-5,.),"09501","09503")
	replace geoco=geoco+"06097" if inrange(substr(geopu,-5,.),"09701","09703")
	replace geoco=geoco+"06099" if inrange(substr(geopu,-5,.),"09901","09904")
	replace geoco=geoco+"06107" if inrange(substr(geopu,-5,.),"10701","10703")
	replace geoco=geoco+"06111" if inrange(substr(geopu,-5,.),"11101","11106")
	for any "w" "b" "n" "a" "p" "o" "m": assert Xh_shrh<. \\ gen Xh_dec=round(rtoth*Xh_shrh)
	egen chk=rowtotal(wh_dec bh_dec nh_dec ah_dec ph_dec oh_dec mh_dec) 
	gen diff=rtoth-chk
	sum diff if diff!=0
	while `r(N)'>0 { 
		gen draw=runiform() if diff!=0 
		replace wh_dec=wh_dec+1 if inrange(draw,0,wh_shrh) & diff>0
		replace bh_dec=bh_dec+1 if inrange(draw,wh_shrh,wh_shrh+bh_shrh) & diff>0
		replace nh_dec=nh_dec+1 if inrange(draw,wh_shrh+bh_shrh,wh_shrh+bh_shrh+nh_shrh) & diff>0
		replace ah_dec=ah_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh) & diff>0
		replace ph_dec=ph_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh) & diff>0
		replace oh_dec=oh_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh) & diff>0
		replace mh_dec=mh_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh+mh_shrh) & diff>0
		replace wh_dec=wh_dec-1 if inrange(draw,0,wh_shrh) & diff<0
		replace bh_dec=bh_dec-1 if inrange(draw,wh_shrh,wh_shrh+bh_shrh) & diff<0
		replace nh_dec=nh_dec-1 if inrange(draw,wh_shrh+bh_shrh,wh_shrh+bh_shrh+nh_shrh) & diff<0
		replace ah_dec=ah_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh) & diff<0
		replace ph_dec=ph_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh) & diff<0
		replace oh_dec=oh_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh) & diff<0
		replace mh_dec=mh_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh+mh_shrh) & diff<0
		drop chk diff draw
		egen chk=rowtotal(wh_dec bh_dec nh_dec ah_dec ph_dec oh_dec mh_dec) 
		gen diff=rtoth-chk
		sum diff if diff!=0		
	}
	drop chk diff
	save "$path/data.working/acs19_puma.dta", replace // puma control
	// 5ACS19 county pop by race
	use if statea=="06" using "$path/data.acs/nhgis_co_5acs19.dta", clear
	bigtab county countya aluke001, nocum
	gen geoco="05000US"+statea+countya
	keep geoco aluk*
	# delimit ;
	ren aluke001 totpop	;	ren alukm001 totpop_moe	;
	ren aluke002 totnh	;	ren alukm002 totnh_moe  ;
	ren aluke003 wnh	;	ren alukm003 wnh_moe    ;
	ren aluke004 bnh	;   ren alukm004 bnh_moe    ;
	ren aluke005 nnh	;   ren alukm005 nnh_moe    ;
	ren aluke006 anh	;   ren alukm006 anh_moe    ;
	ren aluke007 pnh	;   ren alukm007 pnh_moe    ;
	ren aluke008 onh	;   ren alukm008 onh_moe    ;
	ren aluke009 mnh	;   ren alukm009 mnh_moe    ;
	ren aluke012 toth	;   ren alukm012 toth_moe   ;
	ren aluke013 wh		;	ren alukm013 wh_moe     ;
	ren aluke014 bh		;   ren alukm014 bh_moe     ;
	ren aluke015 nh		;   ren alukm015 nh_moe     ;
	ren aluke016 ah		;   ren alukm016 ah_moe     ;
	ren aluke017 ph		;   ren alukm017 ph_moe     ;
	ren aluke018 oh		;   ren alukm018 oh_moe     ;
	ren aluke019 mh		;   ren alukm019 mh_moe     ;
	#delimit cr
	drop aluk*
	order geoco
	** replace hispanic by race with DEC10 based shares
	merge 1:1 geoco using "$path/data.working/dec_co_hisprace.dta", assert(3 4 5) keepus(*shrh) nogen update 
	for any "w" "b" "n" "a" "p" "o" "m": assert Xh_shrh<. \\ gen Xh_dec=round(toth*Xh_shrh)
	egen chk=rowtotal(wh_dec bh_dec nh_dec ah_dec ph_dec oh_dec mh_dec) 
	gen diff=toth-chk
	sum diff if diff!=0
	while `r(N)'>0 { 
		gen draw=runiform() if diff!=0 
		replace wh_dec=wh_dec+1 if inrange(draw,0,wh_shrh) & diff>0
		replace bh_dec=bh_dec+1 if inrange(draw,wh_shrh,wh_shrh+bh_shrh) & diff>0
		replace nh_dec=nh_dec+1 if inrange(draw,wh_shrh+bh_shrh,wh_shrh+bh_shrh+nh_shrh) & diff>0
		replace ah_dec=ah_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh) & diff>0
		replace ph_dec=ph_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh) & diff>0
		replace oh_dec=oh_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh) & diff>0
		replace mh_dec=mh_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh+mh_shrh) & diff>0
		replace wh_dec=wh_dec-1 if inrange(draw,0,wh_shrh) & diff<0
		replace bh_dec=bh_dec-1 if inrange(draw,wh_shrh,wh_shrh+bh_shrh) & diff<0
		replace nh_dec=nh_dec-1 if inrange(draw,wh_shrh+bh_shrh,wh_shrh+bh_shrh+nh_shrh) & diff<0
		replace ah_dec=ah_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh) & diff<0
		replace ph_dec=ph_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh) & diff<0
		replace oh_dec=oh_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh) & diff<0
		replace mh_dec=mh_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh+mh_shrh) & diff<0
		drop chk diff draw
		egen chk=rowtotal(wh_dec bh_dec nh_dec ah_dec ph_dec oh_dec mh_dec) 
		gen diff=toth-chk
		sum diff if diff!=0		
	}
	drop chk diff
	save "$path/data.working/acs19_co.dta", replace
	// 5ACS19 state pop by race
	use if statea=="06" using "$path/data.acs/nhgis_st_5acs19.dta", clear
	gen geost="04000US"+statea
	keep geost aluk*
	# delimit ;
	ren aluke001 totpop	;	ren alukm001 totpop_moe	;
	ren aluke002 totnh	;	ren alukm002 totnh_moe  ;
	ren aluke003 wnh	;	ren alukm003 wnh_moe    ;
	ren aluke004 bnh	;   ren alukm004 bnh_moe    ;
	ren aluke005 nnh	;   ren alukm005 nnh_moe    ;
	ren aluke006 anh	;   ren alukm006 anh_moe    ;
	ren aluke007 pnh	;   ren alukm007 pnh_moe    ;
	ren aluke008 onh	;   ren alukm008 onh_moe    ;
	ren aluke009 mnh	;   ren alukm009 mnh_moe    ;
	ren aluke012 toth	;   ren alukm012 toth_moe   ;
	ren aluke013 wh		;	ren alukm013 wh_moe     ;
	ren aluke014 bh		;   ren alukm014 bh_moe     ;
	ren aluke015 nh		;   ren alukm015 nh_moe     ;
	ren aluke016 ah		;   ren alukm016 ah_moe     ;
	ren aluke017 ph		;   ren alukm017 ph_moe     ;
	ren aluke018 oh		;   ren alukm018 oh_moe     ;
	ren aluke019 mh		;   ren alukm019 mh_moe     ;
	#delimit cr
	drop aluk*
	order geost
	** replace hispanic by race with DEC10 based shares
	merge 1:1 geost using "$path/data.working/dec_st_hisprace.dta", assert(3 4 5) keepus(*shrh) nogen update 
	for any "w" "b" "n" "a" "p" "o" "m": assert Xh_shrh<. \\ gen Xh_dec=round(toth*Xh_shrh)
	egen chk=rowtotal(wh_dec bh_dec nh_dec ah_dec ph_dec oh_dec mh_dec) 
	gen diff=toth-chk
	sum diff if diff!=0
	while `r(N)'>0 { 
		gen draw=runiform() if diff!=0 
		replace wh_dec=wh_dec+1 if inrange(draw,0,wh_shrh) & diff>0
		replace bh_dec=bh_dec+1 if inrange(draw,wh_shrh,wh_shrh+bh_shrh) & diff>0
		replace nh_dec=nh_dec+1 if inrange(draw,wh_shrh+bh_shrh,wh_shrh+bh_shrh+nh_shrh) & diff>0
		replace ah_dec=ah_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh) & diff>0
		replace ph_dec=ph_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh) & diff>0
		replace oh_dec=oh_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh) & diff>0
		replace mh_dec=mh_dec+1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh+mh_shrh) & diff>0
		replace wh_dec=wh_dec-1 if inrange(draw,0,wh_shrh) & diff<0
		replace bh_dec=bh_dec-1 if inrange(draw,wh_shrh,wh_shrh+bh_shrh) & diff<0
		replace nh_dec=nh_dec-1 if inrange(draw,wh_shrh+bh_shrh,wh_shrh+bh_shrh+nh_shrh) & diff<0
		replace ah_dec=ah_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh) & diff<0
		replace ph_dec=ph_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh) & diff<0
		replace oh_dec=oh_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh) & diff<0
		replace mh_dec=mh_dec-1 if inrange(draw,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh,wh_shrh+bh_shrh+nh_shrh+ah_shrh+ph_shrh+oh_shrh+mh_shrh) & diff<0
		drop chk diff draw
		egen chk=rowtotal(wh_dec bh_dec nh_dec ah_dec ph_dec oh_dec mh_dec) 
		gen diff=toth-chk
		sum diff if diff!=0		
	}
	drop chk diff
	save "$path/data.working/acs19_st.dta", replace 

/************************************
RAKING/POSTSTRATIFICATION
purpose: impose top-down consistency with higher geographical totals
************************************/
	
// add geoids to blocks (sumlev 50 157 971 from USCB block assignment file)
	// block 
	** make sure it's 2010! the file names are the same each decade.
	import delim using "$path/data.geo/BlockAssign_ST06_CA_INCPLACE_CDP.txt", clear 
	format blockid %15.0f
	gen geobk=string(blockid,"%015.0f")
	replace geobk=trim("10100US"+geobk)
	// place 157 (convert from 160) 
	** merge twice with state estimates, first keeping nonmatching place codes, then matching.
	// first, manually add Jurupa Valley, CA which was not incorporated in 2010. geoid=15700US0606537692
	// accomplished by: loading city gis bounds, selecting 2010 blocks inside bounds.
	gen GEOID10=substr(geobk,8,.) // numeric part only
	destring GEOID10, replace
	merge 1:1 GEOID10 using "$path/data.dru/jurupa_valley_blocks.dta", assert(1 3) keepus(placenew) 
	destring placenew, replace
	replace placefp=placenew if _merge==3 // update to add jurupa valley blocks
	drop _merge	placenew GEOID10
	// now gen full place codes
	gen geopl=trim("15700US"+substr(geobk,8,5)+string(placefp,"%05.0f")) if placefp<. // actualy file contains sumlev 160 (incorp or CDPs) but converted to 157 (place w/in county or balance)
	tempfile tmp
	preserve
	merge m:1 geopl using "$path/data.working/pldru20.dta", keep(1) nogen // identify 160 places that are not in 157
	replace geopl="15700US06"+substr(geobk,10,3)+"99990"
	save `tmp', replace
	restore
	merge m:1 geopl using "$path/data.working/pldru20.dta", keep(3) nogen // keep common 157 place codes
	append using `tmp'
	merge 1:1 geobk using "$path/bda_bk3.dta" , assert(3) nogen // here, some geopu are added ~ but not all. causes problem below.
	merge m:1 geopl using "$path/data.working/pldru20.dta", assert(3 4) nogen keepus(druest20 drugq20) update // update w/missing popn totals for 99990 blocks
	// co 050
	cap gen geoco="05000US06"+substr(geobk,10,3)
	// puma 971
	// keep only to merge fips with >1 pumas
	cap gen geotr="14000US"+substr(geobk,8,11)
	cap drop geopu 
	merge m:1 geotr using "$path/data.census/tr_to_puma.dta", keepus(geopu) assert(3) 
	replace geopu="" if !inlist(substr(geoco,10,3),"001","007","013","019","029","037","041","047","053") & /// 
						!inlist(substr(geoco,10,3),"059","061","065","067","071","073","075","077","079") & ///
						!inlist(substr(geoco,10,3),"081","083","085","087","095","097","099","107","111") // all counties with >1 puma
	bigtab geoco geopu // keep pu codes for merging data by multipuma counties only.
	// cleanup                                                                                                                  
	order geobk geobg geotr geopl geopu geoco                                                                                   
	keep geobk geobg geotr geopl geopu geoco ///                                                                                
		tothu19 hhpop19 totgq19 gqpop19 ///                                                                                     
		wnh wh bnh bh nnh nh anh ah pnh ph onh oh mnh mh                                                                        
	save "$path/bda_bk4.dta", replace // contains all geoids and uncontrolled projections.                                       
                                                                                                                                
// control order:                                                                                                               
	**A. gqpop                                                                                                                  
	** 1 total GQ facilities by various geosumlev.                                                                              
	**B. total pop:                                                                                                             
	** household (HH gold standard = state total less PEP GQ) and GQ pop (GQ gold standard = PEP county)                        
	** 1 DEC state -> PEP county                                                                                                
	** 2 PEP county -> PRC place                                                                                                
	** 3 PEP county -> ACS PUMA (counties w/2+ PUMAs)                                                                           
	** 4 ACS county/puma -> ACS tract                                                                                           
	** 5 ACS tract + PRC place -> BDA block (first zero out block if tothu19==0)                                                
	**C. pop by race                                                                                                            
	** ACS used for non-hispanic races and total hispanic; DEC used for hispanic races                                          
	** 1 DEC state -> PEP county (above) -> ACS county                                                                          
	** 2 ACS county -> ACS puma (if applicable)                                                                                 
	** 3 ACS county/puma -> ACS tract (fix tract zeros first if DEC nonzero)                                                    
	** 4 ACS tract -> ESRI BG                                                                                                   
	** 5 ESRI BG -> BDA blocks                                                                                                  
	** 6 BDA blocks -> BDA total pop by block (using BDA seeded shares by race).                                                

// A.1. LUCA block GQ counts -> GQ facilities by geosumlev
	use geobk totgq19 using "$path/data.working/luca_bk.dta", clear
	gen geotr="14000US"+substr(geobk,8,11) 
	collapse (sum) totgq19, by(geotr)
	save "$path/data.working/tr_gqs.dta", replace
	
// B.1. DEC state -> PEP total by county 
	// state total: 4237256 hh: 4148607
	use "$path/data.working/copep20.dta", clear
	gen geost="04000US06"
	merge m:1 geost using "$path/data.working/stdec20.dta", assert(3) nogen keepus(resident2020)
	egen gq2020=sum(pepgq2020) 
	gen household2020=resident2020-gq2020
	gen pephh2020=pepest2020-pepgq2020
	survwgt poststratify pephh2020, by(geost) totvar(household2020) gen(hh_co)
	ren pepgq2020 gq_co
	label var hh_co "final weight: county household pop"
	label var gq_co "final weight: county gq pop"
	save "$path/bda_co.dta", replace
	table geost, contents(mean resident2020 sum gq_co mean household2020 sum hh_co) format(%10.0fc) // OK 
	
// B.2 PEP total by county -> dru total by city 
	use "$path/data.working/pldru20.dta", clear
	gen druhh20=druest20-drugq20
	gen geoco="05000US"+substr(geopl,8,5)
	merge m:1 geoco using "$path/bda_co.dta", assert(3) keepus(hh_co gq_co) // _m==2 means the co has no places, those were special cases handled above
	assert hh_co<. & gq_co<.
	survwgt poststratify druhh20, by(geoco) totvar(hh_co) gen(hh_pl) 
	replace hh_pl=0 if hh_pl==.
	label var hh_pl "final weight: place household pop"
	save "$path/bda_pl.dta", replace
	gcollapse (mean) hh_co (sum) hh_pl, by(geoco)
	table geoco, contents(sum hh_co sum hh_pl) format(%9.0fc) row // OK
	
// B.3 PEP total by county -> ACS PUMA by county
	use "$path/data.working/acs19_puma.dta", clear
	cap drop _merge
	merge m:1 geoco using "$path/bda_co.dta", assert(2 3) keep(3) keepus(hh_co gq_co) // nogen
	assert hh_co<. & gq_co<.
	survwgt poststratify hhpop, by(geoco) totvar(hh_co) gen(hh_pu) 
	survwgt poststratify gqpop, by(geoco) totvar(gq_co) gen(gq_pu)
	for var hh_pu gq_pu: replace X=0 if X==.
	label var hh_pu "final weight: PUMA household pop"
	label var gq_pu "final weight: PUMA gq pop"
	save "$path/bda_pu.dta", replace
	gcollapse (mean) hh_co gq_co (sum) hh_pu gq_pu, by(geoco)
	table geoco, contents(sum gq_co sum gq_pu sum hh_co sum hh_pu) row format(%9.0fc)

// B.4 dru county or ACS puma -> ACS tracts
	use "$path/data.working/acs19_tract.dta", clear
	merge 1:1 geotr using "$path/data.census/tr_to_puma.dta", assert(3) keepus(geopu) nogen // all pumas
	merge m:1 geopu using "$path/bda_pu.dta", assert(1 3) keep(1 3) keepus(hh_pu gq_pu) // only pumas of large counties
	assert hh_pu<. & gq_pu<. if _merge==3
	gen touse=1 if _merge==3
	drop _merge
	tab geoco if touse==1
	merge m:1 geoco using "$path/bda_co.dta", assert(3) keepus(hh_co gq_co) nogen
	assert hh_co<. & gq_co<. 
	bigtab geoco geopu if touse 
	survwgt poststratify hhpop if touse==1, by(geopu) totvar(hh_pu) gen(hh_tr)
	survwgt poststratify hhpop if touse!=1, by(geoco) totvar(hh_co) gen(hh_tr2) 
	replace hh_tr=hh_tr2 if touse!=1 
	drop hh_tr2
	replace hh_tr=0 if hh_tr==.
	label var hh_tr "final weight: tract household pop"
	merge 1:1 geotr using "$path/data.working/tr_gqs.dta", assert(3) keepus(totgq19) nogen // luca N of GQs
	assert totgq19<.
	replace gqpop=0 if totgq19==0 // delete tract GQ population if LUCA says no GQs in tract.
	survwgt poststratify gqpop if touse==1, by(geopu) totvar(gq_pu) gen(gq_tr)
	survwgt poststratify gqpop if touse!=1, by(geoco) totvar(gq_co) gen(gq_tr2) 
	replace gq_tr=gq_tr2 if touse!=1 
	drop gq_tr2
	replace gq_tr=0 if gq_tr==.
	assert gq_tr==0 if totgq19==0
	label var gq_tr "final weight: tract gq pop"
	save "$path/bda_tr.dta", replace
	gcollapse (mean) hh_co gq_co (sum) hh_tr gq_tr, by(geoco)
	table geoco, contents(sum gq_co sum gq_tr sum hh_co sum hh_tr) row format(%9.0fc)

// B.5 ACS tracts + dru places -> BDA blocks
	** tracts + places for households; counties + puma for GQs.
	use "$path/bda_bk4.dta", clear // extrapolated block hhpop19 gqpop19 + races
	for any "hh_tr" "hh_pl" "hh_bk" "gq_pu" "gq_co" "gq_bk": cap drop X
	assert hhpop19<. & gqpop19<. & tothu19<. & totgq19<.
	merge m:1 geotr using "$path/bda_tr.dta", assert(3) keepus(hh_tr) nogen 
	merge m:1 geopl using "$path/bda_pl.dta", assert(3) keepus(hh_pl) nogen 
	merge m:1 geopu using "$path/bda_pu.dta", assert(1 3) keepus(gq_pu) nogen 
	merge m:1 geoco using "$path/bda_co.dta", assert(3) keepus(gq_co) nogen 
	assert hh_pl<. & hh_tr<. & gq_co<.
	** reweight gq to county/puma control
	gen touse=gq_pu<.
	survwgt poststratify gqpop19 if touse==1, by(geopu) totvar(gq_pu) gen(gq_bk)
	survwgt poststratify gqpop19 if touse!=1, by(geoco) totvar(gq_co) gen(gq_bk2)
	replace gq_bk=gq_bk2 if gq_bk==. & gq_bk2<.
	drop gq_bk2 touse
	replace gq_bk=0 if gq_bk==.
	assert gq_bk==0 if totgq19==0
	** reweight hh to tract + place controls
	survwgt poststratify hhpop19, by(geotr) totvar(hh_tr) gen(hh_tmp) // first match block pop to acs tract sum
	survwgt poststratify hh_tmp, by(geopl) totvar(hh_pl) gen(hh_bk) // then match block pop to city/balance of county sum
	drop hh_tmp
	replace hh_bk=0 if hh_bk==.
	label var hh_bk "final weight: block household pop"	
	label var gq_bk "final weight: block gq pop"
	save "$path/bda_bk5.dta", replace
	gcollapse (sum) hh_bk gq_bk, by(geoco)
	merge m:1 geoco using "$path/bda_co.dta", assert(3) nogen keepus(hh_co gq_co)
	table geoco, contents(sum gq_co sum gq_bk sum hh_co sum hh_bk) row format(%9.0fc) // problem in 6053 and 6069, county totals don't match

// C.1 PEP county total -> ACS county by race 
	use geoco wnh bnh nnh anh pnh onh mnh wh_dec bh_dec nh_dec ah_dec ph_dec oh_dec mh_dec using "$path/data.working/acs19_co.dta", clear
	rename *_dec * // use DEC10-assigned hispanic races
	merge 1:1 geoco using "$path/bda_co.dta", assert(3) nogen keepus(*_co)
	keep geoco wnh-mnh wh-mh hh_co gq_co
	ren *h pop_*h
	gen tot_co=hh_co+gq_co
	greshape long pop_, i(geoco tot_co hh_co gq_co) j(re) string
	assert re!="co" // if erroneous reshape
	survwgt poststratify pop_, by(geoco) totvar(tot_co) gen(pop_cor)
	label var pop_cor "final weight: county pop by race"
	save "$path/bda_cor.dta", replace
	table geoco re, contents(sum pop_cor) format(%9.0fc) row col // OK 39,538,222 (official: 39,538,223)
	
// C.2 ACS county by race -> ACS PUMA by race (counties 2+ pumas)
	use geopu wnh bnh nnh anh pnh onh mnh wh_dec bh_dec nh_dec ah_dec ph_dec oh_dec mh_dec using "$path/data.working/acs19_puma.dta", clear
	rename *_dec *
	ren *h pop_*h
	greshape long pop_, i(geopu) j(re) string
	gen geoco="05000US"
	replace geoco=geoco+"06001" if inrange(substr(geopu,-5,.),"00101","00110")
	replace geoco=geoco+"06007" if inrange(substr(geopu,-5,.),"00701","00702")
	replace geoco=geoco+"06013" if inrange(substr(geopu,-5,.),"01301","01309")
	replace geoco=geoco+"06019" if inrange(substr(geopu,-5,.),"01901","01907")
	replace geoco=geoco+"06029" if inrange(substr(geopu,-5,.),"02901","02905")
	replace geoco=geoco+"06037" if inrange(substr(geopu,-5,.),"03701","03769")
	replace geoco=geoco+"06041" if inrange(substr(geopu,-5,.),"04101","04102")
	replace geoco=geoco+"06047" if inrange(substr(geopu,-5,.),"04701","04702")
	replace geoco=geoco+"06053" if inrange(substr(geopu,-5,.),"05301","05303")
	replace geoco=geoco+"06059" if inrange(substr(geopu,-5,.),"05901","05918")
	replace geoco=geoco+"06061" if inrange(substr(geopu,-5,.),"06101","06103")
	replace geoco=geoco+"06065" if inrange(substr(geopu,-5,.),"06501","06515")
	replace geoco=geoco+"06067" if inrange(substr(geopu,-5,.),"06701","06712")
	replace geoco=geoco+"06071" if inrange(substr(geopu,-5,.),"07101","07115")
	replace geoco=geoco+"06073" if inrange(substr(geopu,-5,.),"07301","07322")
	replace geoco=geoco+"06075" if inrange(substr(geopu,-5,.),"07501","07507")
	replace geoco=geoco+"06077" if inrange(substr(geopu,-5,.),"07701","07704")
	replace geoco=geoco+"06079" if inrange(substr(geopu,-5,.),"07901","07902")
	replace geoco=geoco+"06081" if inrange(substr(geopu,-5,.),"08101","08106")
	replace geoco=geoco+"06083" if inrange(substr(geopu,-5,.),"08301","08303")
	replace geoco=geoco+"06085" if inrange(substr(geopu,-5,.),"08501","08514")
	replace geoco=geoco+"06087" if inrange(substr(geopu,-5,.),"08701","08702")
	replace geoco=geoco+"06095" if inrange(substr(geopu,-5,.),"09501","09503")
	replace geoco=geoco+"06097" if inrange(substr(geopu,-5,.),"09701","09703")
	replace geoco=geoco+"06099" if inrange(substr(geopu,-5,.),"09901","09904")
	replace geoco=geoco+"06107" if inrange(substr(geopu,-5,.),"10701","10703")
	replace geoco=geoco+"06111" if inrange(substr(geopu,-5,.),"11101","11106")
	keep if inlist(substr(geoco,10,3),"001","007","013","019","029","037","041","047","053") | ///
			inlist(substr(geoco,10,3),"059","061","065","067","071","073","075","077","079") | ///
			inlist(substr(geoco,10,3),"081","083","085","087","095","097","099","107","111")
	merge m:1 geoco re using "$path/bda_cor.dta", assert(2 3) keep(3) keepus(pop_cor) nogen
	assert pop_<.
	survwgt poststratify pop_, by(geoco re) totvar(pop_cor) gen(pop_pur)
	label var pop_pur "final weight: puma pop by race"
	save "$path/bda_pur.dta", replace
	table geoco re , contents(sum pop_pur) format(%9.0fc) row col // 37,234,192 persons living in counties w/>1 puma
	
// C.3 ACS county/puma re -> ACS tract re (adding dummy exposure in place of ACS tract 'zeros')
	use geotr wnh bnh nnh anh pnh onh mnh wh_dec bh_dec nh_dec ah_dec ph_dec oh_dec mh_dec using "$path/data.working/acs19_tract.dta", clear
	rename *_dec *
	ren *h pop_*h
	greshape long pop_, i(geotr) j(re) string
	merge m:1 geotr using "$path/data.census/tr_to_puma.dta", assert(3) keepus(geopu) nogen
	gen geoco="05000US"+substr(geotr,8,5)
	merge m:1 geopu re using "$path/bda_pur.dta", assert(1 3) keep(1 3) keepus(pop_pur) 
	gen touse=1 if _merge==3
	drop _merge
	merge m:1 geoco re using "$path/bda_cor.dta", assert(1 3) keep(1 3) keepus(pop_cor) nogen
	survwgt poststratify pop_ if touse==1, by(geopu re) totvar(pop_pur) gen(pop_trr)
	survwgt poststratify pop_ if touse!=1, by(geoco re) totvar(pop_cor) gen(pop_trr2)
	replace pop_trr=pop_trr2 if touse!=1 
	survwgt poststratify pop_trr if inlist(geoco,"05000US06053","05000US06069"), by(geoco re) totvar(pop_cor) gen(pop_fix) // error in 6069 6053 !!
	replace pop_trr=pop_fix if inlist(geoco,"05000US06053","05000US06069")
	drop pop_trr2 pop_fix
	label var pop_trr "final weight: tract pop by race"
	save "$path/bda_trr.dta", replace
	gcollapse (mean) pop_cor (sum) pop_trr, by(geoco re) 
	table geoco re , contents(sum pop_cor sum pop_trr) format(%9.0fc) row col // OK 39,538,221 (official: 39,538,223)

// C.4 + C.5 ACS tracts by re -> ACS BGs by re (skip) -> BDA blocks by re 
	use geobk geobg geotr geopu geopl geoco wnh bnh nnh anh pnh onh mnh wh bh nh ah ph oh mh using "$path/bda_bk5.dta", clear // bda starts with DEC10, so no _dec suffix.
	rename *h pop_*h
	greshape long pop_, i(geobk geobg geotr geopu geopl geoco) j(re) string
	assert pop_<.
	merge m:1 geotr re using "$path/bda_trr.dta", assert(3) nogen keepus(pop_trr) 
	assert pop_<.
	replace pop_=1e-3 if pop_==0 // dummy exposure ~ at least some probability of selection for every race w/in a BG.
	survwgt poststratify pop_, by(geotr re) totvar(pop_trr) gen(pop_bkr)
	replace pop_=0 if pop_>=.0009 & pop_<.0011 // unused slots
	label var pop_bkr "final weight: BK pop by race" 
	zipsave "$path/bda_bkr.dta", replace
	merge m:1 geoco re using "$path/bda_cor.dta", assert(3) nogen keepus(pop_cor) 
	gcollapse (mean) pop_cor (sum) pop_bkr, by(geoco re)
	table geoco re, contents(sum pop_cor sum pop_bkr) format(%9.0fc) row col // OK 39,538,221 (official: 39,538,223)

// C.6 prior BDA block total -> BDA block pop by race 
	zipuse geobk geoco geopl geotr re pop_bkr using "$path/bda_bkr.dta", clear // controlled block by race
	merge m:1 geobk using "$path/bda_bk5.dta", keepus(gq_bk hh_bk tothu19 totgq19) assert(3) nogen // add block control totals
	gen totpop_bk=hh_bk+gq_bk
	assert totpop_bk<. & tothu19<. & totgq19<.
	*egen chk=sum(pop_bkr), by(geobk) // race totals by block before control
	survwgt poststratify pop_bkr, by(geobk) totvar(totpop_bk) gen(pop_bkr_tot) 
	replace pop_bkr_tot=0 if pop_bkr_tot==.
	*egen chk2=sum(pop_bkr_tot), by(geobk)
	*drop chk chk2
	*browse geobk geoco re pop_bkr chk totpop_bk gq_bk hh_bk pop_bkr_tot chk2 
	label var totpop_bk "final weight: block total pop"
	label var pop_bkr_tot "final weight: BK pop by race (controlled to totpop)"
	zipsave "$path/bda_bkr.dta", replace
	gcollapse (mean) gq_bk hh_bk totpop_bk (sum) pop_bkr pop_bkr_tot, by(geobk geoco)
	table geoco, contents(sum gq_bk sum hh_bk sum totpop_bk sum pop_bkr sum pop_bkr_tot) row format(%9.0fc) 
	zipuse "$path/bda_bkr.dta", clear
	gcollapse (sum) pop_bkr pop_bkr_tot, by(geoco re)
	table re, contents(sum pop_bkr sum pop_bkr_tot) row col format(%9.0fc)
	// total: 39,538,221 (should be 39,538,223) <<-- pop_bkr is closer than pop_bkr_tot. HOWEVER, pop_bkr is inconsistent with tothu19.
	// gq: 829,974 
	// hh: 38,708,248 
	/* official:	total 	39,538,223	
					wnh 	13,714,587	
					bnh 	 2,119,286	
					aiannh 	   156,085	
					anh 	 5,978,795	
					nhpinh 	   138,167	
					sornh 	   223,929	
					tomnh 	 1,627,722	
					hispan 	15,579,652 	*/

// cleanup and export
	** tbd: replace N~3 blocks w/o LUCA data with DEC data.
	zipuse geobk re pop_bkr_tot using "$path/bda_bkr.dta", clear
	greshape wide pop_bkr_tot, i(geobk) j(re) string
	rename pop_bkr_tot* *
	merge 1:1 geobk using "$path/bda_bk5.dta", keepus(geo* totgq19 gq_bk tothu19 hh_bk) assert(3) nogen
	gen totpop_bk=hh_bk+gq_bk
	assert totpop_bk<.
	egen chk=rowtotal(wnh bnh nnh anh pnh onh mnh wh bh nh ah ph oh mh)
	cap assert inrange(chk,totpop_bk-1,totpop_bk+1) 
	if _rc {
		nois list geobk tothu19 totgq19 gq_bk hh_bk totpop_bk chk if !inrange(chk,totpop_bk-1,totpop_bk+1), clean noobs
		assert inrange(chk,totpop_bk-5,totpop_bk+5) 
		replace onh=totpop_bk if chk==0 & totpop_bk>0 // arbitrarily assign to onh
	}
	drop chk
	gen geost="04000US06"
	order geobk geobg geotr geopl geopu geoco geost ///
		totgq19 gq_bk tothu19 hh_bk ///
		totpop_bk wnh bnh nnh anh pnh onh mnh wh bh nh ah ph oh mh
	compress
	save "$path/bda_bk10.dta", replace
	export delim "$path/bda_bk10.csv", replace
	zipfile file "bda_bk10.csv", saving("bda_bk10.csv.zip", replace) 
	rm "$path/bda_bk10.csv"

// comparison tables (tbd: merge 2010 decnnial + 2019 acs for comparisons)
	cap log close 
	set linesize 192 
	log using "$path/bda_log.txt", text replace
	nois di _newline "Tables from BDA project:" 
	table geost, contents(sum hh_bk sum gq_bk sum totpop_bk) row format(%10.0fc)
	table geoco, contents(sum hh_bk sum gq_bk sum totpop_bk) row format(%10.0fc)
	table geopl, contents(sum hh_bk sum gq_bk sum totpop_bk) row format(%10.0fc)
	qui log off
	for var wnh bnh nnh anh pnh onh mnh wh bh nh ah ph oh mh: ren X pop_X
	keep geo* totgq19 gq_bk tothu19 hh_bk totpop_bk pop_*
	greshape long pop_, i(geobk geobg geotr geopl geopu geoco geost totgq19 gq_bk tothu19 hh_bk totpop_bk) j(re) string
	qui log on
	table geoco re, contents(sum pop_) col row format(%10.0fc)
	table geopu re, contents(sum pop_) col row format(%10.0fc)
	log close
	set linesize 80

// checks
	// ok gqpop<=totpop
	gen diff=totpop_bk-gq_bk
	assert diff>=-1
	drop diff
	// ok gqpop if no gq facilities
	assert gq_bk==0 if totgq19==0
	// ok no hhpop if no hu
	assert totpop_bk==0 if tothu19==0 & totgq19==0
	// ok hhpop/hu correct pph
	gen hpph=hh_bk/tothu19
	sum hpph, d
	drop hpph
	
/************************************
GEOGRAPHIC ALLOCATION
purpose: convert 2010 census blocks into 2020 blocks using land cover data
************************************/
end

// prep to reallocate between 2010 and 2020 blocks
** methods: nlcd16 (amos) or CEDS2 (allocate HU to residential lots -> footprints and GQ to GQ lots/points by type)
** too many obs -- 1.9gb in memory for CA. so, split into 100k obs at a time.
	import delim using "$path/data.block.convert/block1020_crosswalk_06.csv", clear stringc(_all) // amos "when boundaries collide" based on nlcd
	ren v1 geobk20 
	ren v2 geobk10_1
	ren v3 geobk10_1_factor
	local i=2
	forvalues n=4/377 {
		if mod(`n',2)==0 ren v`n' geobk10_`i'
		if mod(`n',2)==1 {
			ren v`n' geobk10_`i'_factor
			local ++i
		}
	}
	save "$path/data.working/block1020_tmp.dta"
	touch "$path/data.working/block1020.dta", replace
	d,s
	local subsets=ceil(`r(N)'/100000)
	local max=`r(N)'
	local m=0
	forvalues num=1/`subsets' {
		local n=`m'+1
		local m=min(`max',`num'*100000)
		use in `n'/`m' using "$path/data.working/block1020_tmp.dta"
		greshape long geobk10_@ geobk10_@_factor, i(geobk20) j(n)
		drop if geobk10_==""
		rename *_ *
		replace geobk10="10100US"+geobk10
		destring geobk10__factor, replace
		append using "$path/data.working/block1020.dta"
		save "$path/data.working/block1020.dta", replace
	}

// load bda_bk and convert to 2020 boundaries
	use "$path/bda_bk10.dta", clear
	ren geobk geobk10
	merge 1:m geobk10 using "$path/data.working/block1020.dta"
	** confirm 0 persons in unmatched blcoks
	sum totpop_bk if _merge==1
	drop if _merge==1
	drop _merge 
	** confirm race=total
	egen chk=rowtotal(wnh-mh) 
	assert inrange(chk,totpop_bk-1,totpop_bk+1)
	drop totpop_bk
	** factor to allocate into 2020
	rename *h pop_*h
	for var hh_bk gq_bk pop_* tothu19 totgq19: gen X_20=X*geobk10__factor
	gcollapse (sum) hh_bk_20 gq_bk_*20 tothu19_20 totgq19_20 pop*_20, by(geobk20)
	** confirm result OK
	gen geoco="05000US"+substr(geobk20,1,5)
	egen totpop_20=rowtotal(pop_*20) 
	table geoco, contents(sum hh_bk_20 sum gq_bk_20 sum totpop_20) row format(%10.0fc) // OK:  39,538,222 (official:  39,538,223)

// export results 
	rename *_20 *
	drop geoco totpop
	save "$path/bda_bk20.dta", replace
	export delim "$path/bda_bk20.csv", replace
	zipfile file "bda_bk20.csv", saving("bda_bk20.csv.zip", replace) 
	rm "$path/bda_bk20.csv"
	
/************************************
NATURAL NUMBER CONVERSION
purpose: convert to natural numbers and reset consistency with higher level geographic controls
************************************/

// store control totals in memory 
	// state total
	use "$path/data.working/stdec20.dta", clear
	mkmat resident2020, mat(st) rownames(geost)
	// county totals
	use geoco gq_co hh_co using "$path/bda_co.dta", clear 
	sum gq_co, mean
	local fix=el(st,1,1)-`r(sum)' // state hh
	replace hh_co=round(hh_co) // rounded
	sum hh_co, mean
	local tot=`r(sum)'
	local diff=abs(`fix'-`tot')
	while `diff'!=0 {
		confirm integer num `diff'
		gsample `diff' [iw=hh_co] , gen(flag)
		if `fix'>`tot' replace hh_co=hh_co+flag
		else if `fix'<`tot' replace hh_co=hh_co-flag
		cap assert hh_co>=0 & hh_co<.
		if _rc replace hh_co=0 if hh_co<0 // fix negatives
		drop flag
		sum hh_co, mean
		local tot=`r(sum)'
		local diff=abs(`fix'-`tot')
	}
	gen tot_co=gq_co+hh_co
	assert tot_co<.
	table geoco, contents(sum gq_co sum hh_co sum tot_co) row // OK
	mkmat tot_co, mat(co) rownames(geoco) // county tot
	mkmat gq_co, mat(gqco) rownames(geoco) // county gq
	mkmat hh_co, mat(hhco) rownames(geoco) // county hh
	// county pop by race
	use geoco wnh-mh using "$path/bda_bk10.dta", clear
	gcollapse (sum) *h, by(geoco)
	rename *h pop_*h
	greshape long pop_, i(geoco) j(re) string
	replace pop_=round(pop_)
	levelsof geoco, local(geos)
	foreach g of local geos {
		local fix=el("co",rownumb("co","`g'"),1) // county tot
		sum pop_ if geoco=="`g'", mean
		local tot=`r(sum)'
		local diff=abs(`fix'-`tot')
		while `diff'!=0 {
			confirm integer num `diff'
			gsample `diff' if geoco=="`g'" [iw=pop_], gen(flag)
			if `fix'>`tot' replace pop_=pop_+flag if geoco=="`g'"
			else if `fix'<`tot' replace pop_=pop_-flag if geoco=="`g'"
			cap assert pop_>=0 & pop_<.
			if _rc replace pop_=0 if pop_<0 // fix negatives
			drop flag
			sum pop_ if geoco=="`g'", mean
			local tot=`r(sum)'
			local diff=abs(`fix'-`tot')
		}
	}
	table geoco re, contents(sum pop_) row col 
	greshape wide pop_, i(geoco) j(re) string
	mkmat pop*, mat(rco) rownames(geoco) 
	// county hu & gq
	use "$path/data.working/luca_bk.dta", clear 
	gen geoco="05000US"+substr(geobk,8,5)
	gcollapse (sum) tothu19 totgq19, by(geoco)
	mkmat tothu19 totgq19, mat(luca) rownames(geoco)

// round and control to pre-rounded populations.
	// 1. round race and resample to control totals
	use "$path/bda_bk20.dta", clear
	gen geoco="05000US"+substr(geobk20,1,5)
	** begin race loop
	nois di "Resampling population by race to control totals:"
	levelsof geoco, local(geos)
	local i=1
	qui foreach g of local geos {
		nois di _newline "`g'" _cont
		foreach r in "wh" "bh" "nh" "ah" "ph" "oh" "mh" "wnh" "bnh" "nnh" "anh" "pnh" "onh" "mnh" {
			if `i'==1 ren pop_`r' pop_`r'_old
			if `i'==1 gen pop_`r'=round(pop_`r'_old)
			nois di "." _cont
			local fix=el("rco",rownumb("rco","`g'"),colnumb("rco","pop_`r'")) 
			sum pop_`r' if geoco=="`g'", mean
			local tot=`r(sum)'
			local diff=abs(`fix'-`tot')
			while `diff'!=0 {
				confirm integer num `diff'
				gsample `diff' if geoco=="`g'" [iw=pop_`r'_old], gen(flag) // use original Ns.
				if `fix'>`tot' replace pop_`r'=pop_`r'+flag
				else if `fix'<`tot' replace pop_`r'=pop_`r'-flag
				cap assert pop_`r'>=0 & pop_`r'<.
				if _rc replace pop_`r'=0 if pop_`r'<0 // fix negatives
				drop flag
				sum pop_`r' if geoco=="`g'", mean
				local tot=`r(sum)'
				local diff=abs(`fix'-`tot')
			}
		}
		local ++i
	}
	for any "wh" "bh" "nh" "ah" "ph" "oh" "mh" "wnh" "bnh" "nnh" "anh" "pnh" "onh" "mnh": drop pop_X_old
	// 2. rebalance GQpop
	nois di "Resampling GQ pop to county totals, respecting N GQs"
	assert gq_bk<.
	ren gq_bk gq_bk_old
	gen gq_bk=round(gq_bk_old)
	replace gq_bk=0 if totgq19==0
	replace gq_bk_old=0 if totgq19==0
	egen chk=rowtotal(pop_*) 
	replace gq_bk=chk if gq_bk>chk // set max gqpop=totpop
	drop chk
	levelsof geoco, local(geos)
	qui foreach g of local geos {
		nois di "." _cont
		local fix=el("gqco",rownumb("gqco","`g'"),1) // gqpop that should exist
		sum gq_bk if geoco=="`g'", mean // gqpop that were found
		local tot=`r(sum)'
		local diff=abs(`fix'-`tot')
		while `diff'!=0 { // try to allocate GQpop w/in known GQ blocks.
			confirm integer num `diff'
			nois di _newline "county `g': official gq `fix' block gq est `tot' (diff: `diff')"
			replace gq_bk_old=totgq19 if gq_bk_old==0 & totgq19>=1 & geoco=="`g'" // use N gq if gq_bk=0
			egen chk=rowtotal(pop_*) if geoco=="`g'" // total pop
			cap gsample `diff' if geoco=="`g'" & gq_bk<chk [iw=gq_bk_old], gen(flag) // gq <= total
			if _rc { // fallback if insufficient records
				replace gq_bk_old=1e-3 if gq_bk_old==0 & geoco=="`g'" // create novel gqs when necessary
				gsample `diff' if geoco=="`g'" & gq_bk<chk [iw=gq_bk_old], gen(flag)
			}
			if `fix'>`tot' replace gq_bk=gq_bk+flag
			else if `fix'<`tot' replace gq_bk=gq_bk-flag
			assert gq_bk<.
			cap assert gq_bk>=0 
			if _rc replace gq_bk=0 if gq_bk<0 // fix negatives
			cap assert gq_bk<=chk if geoco=="`g'"
			if _rc replace gq_bk=chk if gq_bk>chk & geoco=="`g'" // fix gq>totpop
			drop chk flag
			replace totgq19=1 if gq_bk>=1 & totgq19==0 & geoco=="`g'" // update gq counts if forced to add novel gqpop
			sum gq_bk if geoco=="`g'", mean
			local tot=`r(sum)'
			local diff=abs(`fix'-`tot')
		}
	}
	drop gq_bk_old // n.b. this was modified in the course of reweighting
	egen tot_bk=rowtotal(pop_*) // new total pop
	replace hh_bk=tot_bk-gq_bk // update household pop = total less gq
	assert hh_bk>=0
	// 3. rebalance HU and GQ counts
	nois di "Resampling HU and GQ facility counts to county totals:"
	levelsof geoco, local(geos)
	local i=1
	qui foreach g of local geos {
		nois di "." _cont
		foreach h in "hu" "gq" {
			local j="`h'"
			if "`h'"=="hu" local j="hh" 
			local fix=el("luca",rownumb("luca","`g'"),colnumb("luca","tot`h'19"))
			if `i'==1 assert tot`h'19<.
			if `i'==1 ren tot`h'19 tot`h'19_old
			if `i'==1 gen tot`h'19=round(tot`h'19_old)
			sum tot`h'19 if geoco=="`g'", mean
			local tot=`r(sum)'
			local diff=abs(`fix'-`tot')
			local z=1
			while `diff'!=0 & `z'<15 { // try up to 15 times an dthen give up
				confirm integer num `diff'
				nois di _newline "county `g': official `h' `fix' block `h' est `tot' (diff: `diff')"
				gsample `diff' if geoco=="`g'" [iw=tot`h'19_old], gen(flag) // use original Ns
				if `fix'>`tot' replace tot`h'19=tot`h'19+flag
				else if `fix'<`tot' replace tot`h'19=tot`h'19-flag
				cap assert tot`h'19>=0 & tot`h'19<.
				if _rc replace tot`h'19=0 if tot`h'19<0 // fix negatives
				cap assert tot`h'19>0 if `j'_bk>0 // ensure sufficient facilities
				if _rc replace tot`h'19=1 if `j'_bk>0 & tot`h'19==0 // fix missing facilities
				drop flag
				sum tot`h'19 if geoco=="`g'", mean
				local tot=`r(sum)'
				local diff=abs(`fix'-`tot')
				local ++z
			}
		}
		local ++i
	}
	for any "hu" "gq": drop totX19_old
	
	// clean, checks
	// should be : 39,538,223 total / 829,974 in GQ / 38,708,249 in HU
	table geoco, contents(sum gq_bk sum hh_bk sum tot_bk) row format(%10.0fc) // OK
	// gqpop<=totpop
	gen diff=tot_bk-gq_bk
	assert diff>=-1
	drop diff
	// check hu and gq pop against hu and gq totals
	assert gq_bk==0 if totgq19==0
	assert hh_bk==0 if tothu19==0
	assert tot_bk==0 if tothu19==0 & totgq19==0
	// ok hhpop/hu correct pph
	gen hpph=hh_bk/tothu19
	replace hpph=0 if tothu19==0
	sum hpph, d
	list geobk20 hh_bk gq_bk tot_bk *19 hpph if hpph==`r(max)'

// export
	compress
	save "$path/bda_bk20_gis.dta", replace
	export delim "$path/bda_bk20_gis.csv", replace

/************************************
PRODUCTION
purpose: final data checks and export variables corresponding to the PL94-171 file
************************************/

// final checks; renaming
// merge to tiger 2020 block boundary file
// give names matching pl94 file
	* totpop = P0010001 totpop20
	* tothu  = H0010001 tothu20
	* gqpop  = P0050001 gqpop20
	* onerac = P0010002 wa+ba+na+aa+pa+oa
	* wa	 = P0010003 wa
	* ba	 = P0010004 ba
	* na	 = P0010005 na
	* aa	 = P0010006 aa
	* pa	 = P0010007 pa
	* oa	 = P0010008 oa
	* tomrac = P0010009 ma
	* total  = P0020001 totpop20
	* hisp	 = P0020002 wh+bh+nh+ah+ph+oh+mh
	* nh total  = P0020003 wh+bh+nh+ah+ph+oh
	* nh onerace= P0020004 mh
	* nh whi = P0020005 
	* nh blk = P0020006 
	* nh aian= P0020007 
	* nh asi = P0020008 
	* nh nhpi= P0020009 
	* nh sor = P0020010 
	* nh tom = P0020011 

// merge bk and rbk files (block tot and block race)
	use "$path/bda_bk20_gis.dta", clear
	gen  int P0050001=gq_bk
	gen  int P0010001=tot_bk
	egen int P0010002=rowtotal(pop_w* pop_b* pop_n* pop_a* pop_p* pop_o*)
	gen  int P0010003=pop_wh+pop_wnh
	gen  int P0010004=pop_bh+pop_bnh
	gen  int P0010005=pop_nh+pop_nnh
	gen  int P0010006=pop_ah+pop_anh
	gen  int P0010007=pop_ph+pop_pnh
	gen  int P0010008=pop_oh+pop_onh
	gen  int P0010009=pop_mh+pop_mnh
	gen  int P0020001=tot_bk
	egen int P0020002=rowtotal(pop_wh pop_bh pop_nh pop_ah pop_ph pop_oh pop_mh)
	egen int P0020003=rowtotal(pop_wnh pop_bnh pop_nnh pop_anh pop_pnh pop_onh pop_mnh)
	egen int P0020004=rowtotal(pop_wnh pop_bnh pop_nnh pop_anh pop_pnh pop_onh)
	gen  int P0020005=pop_wnh 
	gen  int P0020006=pop_bnh
	gen  int P0020007=pop_nnh
	gen  int P0020008=pop_anh
	gen  int P0020009=pop_pnh
	gen  int P0020010=pop_onh
	gen  int P0020011=pop_mnh
	for var P*: assert X<.
	save "$path/bda_bk20_gis.dta", replace
	export delim using "$path/bda_bk20_gis.csv", replace quote
	zipfile file "bda_bk20_gis.csv", saving("bda_bk20_gis.csv.zip", replace) 
	rm "$path/bda_bk20_gis.csv"
	export delim geobk20 P* using "$path/bda_bk20_gis_pl.csv", replace quote
	zipfile file "bda_bk20_gis_pl.csv", saving("bda_bk20_gis_pl.csv.zip", replace) 
	rm "$path/bda_bk20_gis_pl.csv"
	
