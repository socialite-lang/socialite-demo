MAX_NPI_ID = 880643
MAX_HCPCS_CODE_ID = 5948

# This table stores NPI and their claim information
`NpiClaims(int npi:0..$MAX_NPI_ID, int specialty, (int code, int freq)) indexby npi, sortby code.
 NpiClaims(npi, specialty, code, $sum(freq)) :- l=$read("data/npi-cpt-code.txt"), 
                                  (_npi,_speccial,_code,_freq)=$split(l, "\t"),
                                  npi=$toInt(_npi),
                                  specialty=$toInt(_speccial),
                                  code=$toInt(_code),
                                  freq=$toInt(_freq).`

# This table stores CPT code id to its description
`CptCode(int code:0..$MAX_NPI_ID, String descr).
 CptCode(code, descr) :- l=$read("data/hcpcs-code.txt"),
                         (_, descr, codeStr)=$split(l, "\t"),
                         code=$toInt(codeStr).`

# This computes the total counts by all NPIs for each CPT code
`HcpcsFreq(int code:0..$MAX_NPI_ID, int freq) indexby code.
 HcpcsFreq(code, $sum(freq)) :- NpiClaims(npi, spec, code, freq).`

# This finds CPT code that is too frequently used (such as "initial hospital visit")
`TooFreqCode(int code) sortby code.
 TooFreqCode(code) :- HcpcsFreq(code, freq), freq > 30000000.`

# This finds CPT code that is very frequently used (see NpiCodeCnt table)
`VeryFreqCode(int code) sortby code.
 VeryFreqCode(code) :- HcpcsFreq(code, freq), freq > 10000000.`

`drop HcpcsFreq.`
print "computing SqSum"

`NpiCodeSum(int npi:0..$MAX_NPI_ID, int sum).
 NpiCodeSum(npi, $sum(freq)) :- NpiClaims(npi, _, code, freq), !TooFreqCode(code).`

`NpiMostFreqCode(int npi:0..$MAX_NPI_ID, int freq, int code).
 NpiMostFreqCode(npi, $max(freq), code) :- NpiClaims(npi, _, code, freq), !TooFreqCode(code).`

# This finds the total claims per NPI.
# We exclude the claims for the CPT code that is frequently used by most NPIs
# When we find anomalies, we ignore the NPIs who only (mostly) prescribed frequently used CPT code.
`NpiCodeCnt(int npi:0..$MAX_NPI_ID, int cnt).
 NpiCodeCnt(npi, $inc(0)) :- NpiClaims(npi, _, _, _).
 NpiCodeCnt(npi, $inc(1)) :- NpiClaims(npi, _, code, freq), !VeryFreqCode(code).`

`IgnoredNpi(int npi) sortby npi.
 IgnoredNpi(npi) :- NpiCodeCnt(npi, codecnt), codecnt<=2.`

# FreqCodeToNpi makes groups of NPIs who have prescribed for the CPT code enough times.
# We assign NPI to a group represented by a CPT code, 
#    if it is his most frequently claimed code,
#    or if the CPT code is claimed more than 20% of the NPI's total claim.
`FreqCodeToNpi(int code, (int npi:1024)) indexby code, indexby npi.
 FreqCodeToNpi(code, npi) :- NpiMostFreqCode(npi, _, code),
                             !IgnoredNpi(npi).`

`FreqCodeToNpi(code, npi) :- NpiClaims(npi, _, code, freq), 
                             !IgnoredNpi(npi),
                             !TooFreqCode(code), 
                             NpiCodeSum(npi, sum), freq/sum > 0.2 .`

`GroupSize(int code, int size) indexby code.
 GroupSize(code, $inc(1)) :- FreqCodeToNpi(code, npi).`

# computes square sum of CPT code for each NPI.
`SqSumClaim(int npi:0..$MAX_NPI_ID, int squareSum).
 SqSumClaim(npi, $sum(sq)) :- NpiClaims(npi, _, code, freq), !TooFreqCode(code), sq = freq*freq.`

# computes RMS of CPT code.
`CptCodeRMS(int npi:0..$MAX_NPI_ID, float len).
 CptCodeRMS(npi, len) :- SqSumClaim(npi, sum), len=(float)$Math.sqrt((double)sum).`

print "computing Npi Similarity"
f=open("npi-sim-score.txt", 'w')
for code, size in `GroupSize(code, size)`:
    print "Computing Similarity of a group (size:", size,") for CPT code:", code

    `NpiGroup(int npi:4096) indexby npi.
     NpiGroup(npi) :- FreqCodeToNpi($code, npi).`

    # The following computes cosine similarity between npi1 and npi2 (in the same NpiGroup).
    # TooFreqCode filters out CPT code that is used too often by too many NPIs.
    `NpiSimilarity(int npi1:4096, (int npi2:1024, float sim)) indexby npi1.
     NpiSimilarity(npi1, npi2, $sum(s)) :- NpiGroup(npi1), CptCodeRMS(npi1, len1),
                                           NpiClaims(npi1, _, code, freq1), !TooFreqCode(code),
                                           x=freq1/len1, 
                                           NpiGroup(npi2), npi2>npi1,
                                           CptCodeRMS(npi2, len2), npi2>npi1, 
                                           NpiClaims(npi2, _, code, freq2),
                                           y=freq2/len2, 
                                           s=(float)(x*y).`

    # We store the similarity scores for NPIs in this group.
    for npi1, npi2, sim in `NpiSimilarity(npi1, npi2, sim)`:
        if sim>0.69:
            f.write(str(npi1)+"\t"+str(npi2)+"\t"+str(sim)+"\n")

    `clear NpiGroup.
     clear NpiSimilarity.`
