import time
import sys

MAX_NPI_ID = 880643
MAX_HCPCS_CODE_ID = 5948

print "Reading specialty.txt"
MAX_SPECIALTY_ID = 88
# specialty.txt:  specialty-id <tab> description
# Specialty table maps specialty id to its description.
`Specialty(int specialty:0..$MAX_SPECIALTY_ID, String descr).
 Specialty(spec, descr) :- l=$read("ofer-data/data/specialty.txt"), 
                           (descr, _spec) = $split(l, "\t"),
                           spec=$toInt(_spec).`

specialties = []
specialtyDescrs = ["Ophthalmology", "Otolaryngology", "Gastroenterology", "Oral Surgery (dentists only)", 
                   "Dermatology", "Physical Therapist", "Pathology", "Urology", 
                   "Rheumatology", "Obstetrics/Gynecology", "Podiatry", "Hematology",
                   "Gynecological/Oncology", "Psychiatry", "Allergy/Immunology", "Hematology/Oncology",
                   "Anesthesiology", "Pulmonary Disease", "Nephrology"]
#specialtyDescrs = specialtyDescrs[:5]
for descr in specialtyDescrs:
    num, _ = `Specialty(num, $descr)`.next()
    specialties.append(num)

print "Reading the graph"

import time
# graph.txt: doctor-id1 <tab> doctor-id2
# (there's an edge between doctor-id1 and doctor-id2 if they are similar in terms of their prescriptions)
# Graph table stores the edges (source id, target id).
# EdgeCnt table stores the number of neighbor nodes (node id, neighbor count)
s = time.time()
`Graph(int npi:0..$MAX_NPI_ID, (int npi2)) multiset.
 Graph(npi1, npi2) :- l=$read("ofer-data/data/graph.txt"), 
                       (_npi1, _npi2)=$split(l, "\t"),
                        npi1=$toInt(_npi1),
                        npi2=$toInt(_npi2). 

 Graph(npi2, npi1) :- Graph(npi1, npi2). `
#print "Loading time:%.2f sec."%(time.time()-s)
 
`EdgeCnt(int npi:0..$MAX_NPI_ID, int cnt).
 EdgeCnt(npi, $inc(1)) :- Graph(npi, npi2).`

print "Reading npi-cpt-code.txt"

# npi-cpt-code.txt: doctor id <tab> doctor specialty <tab> cpt code <tab> claim count for the cpt code
# Doctor table stores the information (doctor, specialty, cpt code, count)
`Doctor(int npi:0..$MAX_NPI_ID, int specialty, (int code, int freq)).
 Doctor(npi, specialty, code, freq) :- l=$read("ofer-data/data/npi-cpt-code.txt"), 
                                       (_npi, _spec, _code, _freq) = $split(l, "\t"),
                                       npi = $toInt(_npi),
                                       specialty = $toInt(_spec),
                                       code = $toInt(_code),
                                       freq = $toInt(_freq).`


print "Reading hcpcs-code.txt"
# hcpcs_code.txt: cpt-code id <tab> description
# Specialty table maps specialty id to its description.
`Code(int code:0..$MAX_HCPCS_CODE_ID, String descr).
 Code(code, descr) :- l=$read("ofer-data/data/hcpcs-code.txt"), 
                           (_, descr, _code) = $split(l, "\t"),
                           code = $toInt(_code).`

# We go over selected specialties, 
# and run PageRank algorithm with the NPIs with the specialty as the source nodes.
for i in range(len(specialties)):
    clusterSpecialty = specialty = specialties[i]
    specialty_descr = specialtyDescrs[i]
    print "Running Group PageRank..."

    `Seed(int npi) indexby npi.
     Seed(npi) :- Doctor(npi, specialty, _, _), specialty==$specialty.`
    `SeedCnt(int n:0..0, int cnt) groupby(1).
     SeedCnt(0, $inc(1)) :- Seed(npi). `
    
    _, N = `SeedCnt(0, N)`.next()

    # Rank table stores PageRank values for doctors.
    # (doctor-id, iteration #, rank value)
    # The table only stores the values for the recent two iterations.
    `Rank(int npi:0..$MAX_NPI_ID, int i:iter, float rank) groupby(2).`
    #sys.stdout.write("..")

    # Init PageRank values
    # For the neighbor nodes of the SRC, we initialize the PageRank value of the nodes to be 1/degree
    #`DiffSum(int x:0..0, float s) groupby(1).`

    `Rank(npi, 0, $sum(r)) :- Seed(seed), EdgeCnt(seed, cnt),  r = 1.0f/$N/cnt, Graph(seed, npi).`
    for i in range(10):
        # The first body (till semi-colon) is the random jump to the neighbor nodes of the source nodes (with probabality 0.2)
        # The second body is random walk from one node to its neighbor nodes.
        `Rank(n, $i+1, $sum(r)) :- Seed(npi), EdgeCnt(npi, cnt), r=0.2f*1.0f/$N/cnt, Graph(npi, n) ; 
                        :- Graph(n, s), Rank(s, $i, r1), EdgeCnt(s, cnt), r = 0.8f*r1/cnt.`
        #`DiffSum(0, $sum(r)) :- Rank(n, $i, r1), Rank(n, $i+1, r2), r=(r1-r2)*(r1-r2).`
        sys.stdout.write("..")
        sys.stdout.flush()
    print
    
    `MinRank(int i:0..0, float rank) groupby(1).
     MinRank(0, $min(r)) :- Rank(npi, $i, r), Doctor(npi, specialty, _, _),
                            specialty == $specialty.`
    _, minRank = `MinRank(0, r)`.next()
    threshold = minRank
    `MaxRank(int i:0..0, float rank) groupby(1).
     MaxRank(0, $max(rank)) :- Rank(npi, $i, rank).`
    _, maxRank = `MaxRank(0, rank)`.next()
    if threshold == 0: # this is a fallback
        threshold = maxRank*0.01

    # Candidates stores the doctors (NPI) sorted by their PageRank value (descending order)
    `AnomalyCandidate(int npi, float rank) multiset.
     AnomalyCandidate(npi, rank) :- Rank(npi, $i, rank), Doctor(npi, specialty, _, _),
                                specialty != $specialty, rank>=$threshold.`

    # We sort anomalies by their PageRank values
    anomalies=[]
    from heapq import *
    for npi, rank in `AnomalyCandidate(npi, rank)`:
        heappush(anomalies, (1-rank, npi))   

    # and pick top anomaly candidates with high PageRank values
    topAnomalies = []
    while anomalies:
        priority, npi = heappop(anomalies)
        rank = 1-priority
        topAnomalies.append((npi, rank))
        if len(topAnomalies) > 1000:
            break

    # then we find (what we consider) false-positives. 
    # Specialties occuring more than threshold are considered false-positive.
    specialtyCount={}
    falsePositives=set()
    # We also exclude too general specialties
    falsePositives.add(22) # Physician Assistant
    falsePositives.add(55) # Family Practice
    falsePositives.add(68) # Nurse Practitioner 
    for npi, rank in topAnomalies:
        _, specialty, _, _ = `Doctor($npi, specialty, _, _)`.next()
        try: specialtyCount[specialty] += 1
        except: specialtyCount[specialty] = 1
        if specialtyCount[specialty] > 20:
            falsePositives.add(specialty)

    print "+------------------------------------------+"
    print " Anomaly analysis with specialty ", specialty_descr
    print "   Max PageRank value:", maxRank
    print "   Some of unexpected specialties having high PageRank values:"
    for specialty, cnt in specialtyCount.items():
        if specialty in falsePositives:
            continue
        _, descr = `Specialty($specialty, descr)`.next()
        print "     %s:%d"%(descr, cnt)

    # we print a couple of doctors having specialty that is different from the rest doctors in the cluster
    print "   Top Anomalies:"
    anomalyCount = 0
    while topAnomalies:
        npi, rank = topAnomalies.pop(0)
        _, specialty, _, _ = `Doctor($npi, specialty, _, _)`.next()
        if specialty == clusterSpecialty:
            continue
        if specialty in falsePositives:
            continue
        print "      %d.NPI:%s PageRank:%f"%(anomalyCount+1, npi, rank)
        _, descr = `Specialty($specialty, descr)`.next()
        print "      Specialty:", descr
        for _, specialty, code, freq in `Doctor($npi, specialty, code, freq)`:
            _, descr = `Code($code, descr)`.next()
            print "\t  ", descr, ":", freq

        anomalyCount += 1
        if anomalyCount==10: break

    print "-----------------------------\n"
    specialtyCount.clear()

    `clear Rank.
     clear AnomalyCandidate.
     clear Seed.
     clear SeedCnt.
     clear MaxRank.
     clear MinRank.`
