# -----------------------------------------------------------------------------
# Assignment 2
# Jiangwei Wang (19364744)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Processing Q1 (a)
# -----------------------------------------------------------------------------

# Explore the structure of MSD, including size, formats and data types as well

hdfs dfs -ls -R /data/msd | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'

# /data/msd
# |---audio
# |-----attributes
# |-------msd-jmir-area-of-moments-all-v1.0.attributes.csv
# |-------...
# |-------msd-tssd-v1.0.attributes.csv
# |-----features
# |-------msd-jmir-area-of-moments-all-v1.0.csv
# |---------part-00000.csv.gz
# |---------...
# |---------part-00007.csv.gz
# |-------...
# |-------msd-tssd-v1.0.csv
# |---------part-00000.csv.gz
# |---------...
# |---------part-00007.csv.gz
# |-----statistics
# |-------sample_properties.csv.gz
# |---genre
# |-----msd-MAGD-genreAssignment.tsv
# |-----msd-MASD-styleAssignment.tsv 
# |-----msd-topMAGD-genreAssignment.tsv
# |---main
# |-----summary
# |-------analysis.csv.gz
# |-------metadata.csv.gz
# |---tasteprofile
# |-----mismatches
# |-------sid_matches_manually_accepted.txt
# |-------sid_mismatches.txt
# |-----triplets.tsv
# |-------part-00000.tsv.gz
# |-------...
# |-------part-00007.tsv.gz
 
hdfs dfs -du -s -h /data/msd # 12.9 G 

hdfs dfs -du -h /data/msd

# 12.3 G   98.1 G   /data/msd/audio
# 30.1 M   241.0 M  /data/msd/genre
# 174.4 M  1.4 G    /data/msd/main
# 490.4 M  3.8 G    /data/msd/tasteprofile

hdfs dfs -du -h /data/msd/audio

# 103.0 K  824.3 K  /data/msd/audio/attributes
# 12.2 G   97.8 G   /data/msd/audio/features
# 40.3 M   322.1 M  /data/msd/audio/statistics

hdfs dfs -ls /data/msd/audio/attributes | wc -l # 14

hdfs dfs -cat /data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv | head

# Area_Method_of_Moments_Overall_Standard_Deviation_1,real
# Area_Method_of_Moments_Overall_Standard_Deviation_2,real
# Area_Method_of_Moments_Overall_Standard_Deviation_3,real
# Area_Method_of_Moments_Overall_Standard_Deviation_4,real
# Area_Method_of_Moments_Overall_Standard_Deviation_5,real
# Area_Method_of_Moments_Overall_Standard_Deviation_6,real
# Area_Method_of_Moments_Overall_Standard_Deviation_7,real
# Area_Method_of_Moments_Overall_Standard_Deviation_8,real
# Area_Method_of_Moments_Overall_Standard_Deviation_9,real
# Area_Method_of_Moments_Overall_Standard_Deviation_10,real

hdfs dfs -ls /data/msd/audio/features | wc -l #14

hdfs dfs -ls /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv | wc -l # 9

hdfs dfs -cat /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/* | gunzip |  head

# 1.431,6713.0,52600.0,160600000.0,1264000000.0,9943000000.0,7.086e+12,11400000000.0,89730000000.0,3.465e+15,5.252,11580.0,90080.0,-179100000.0,-1396000000.0,-10870000000.0,6.236e+12,12580000000.0,98020000000.0,2.97e+15,'TRMMMYQ128F932D901'
# 0.9864,3361.0,24270.0,40110000.0,287800000.0,2064000000.0,8.837e+11,2596000000.0,18630000000.0,3.232e+14,2.773,5774.0,41490.0,-44600000.0,-320900000.0,-2307000000.0,7.756e+11,2885000000.0,20760000000.0,2.883e+14,'TRMMMKD128F425225D'
# 1.791,6717.0,57790.0,160900000.0,1385000000.0,11910000000.0,7.105e+12,12520000000.0,1.077e+11,4.52e+15,6.43,11600.0,99690.0,-179500000.0,-1544000000.0,-13270000000.0,6.255e+12,13950000000.0,1.2e+11,3.976e+15,'TRMMMRX128F93187D9'
# 2.209,3371.0,34750.0,40350000.0,412300000.0,4210000000.0,8.912e+11,3710000000.0,37900000000.0,9.415e+14,5.734,5792.0,58320.0,-44870000.0,-454600000.0,-4603000000.0,7.828e+11,4083000000.0,41370000000.0,8.199e+14,'TRMMMCH128F425532C'
# 0.6846,6708.0,30690.0,160400000.0,748900000.0,3492000000.0,7.073e+12,6726000000.0,31380000000.0,7.235e+14,2.485,11580.0,56090.0,-179000000.0,-854900000.0,-4083000000.0,6.227e+12,7670000000.0,36660000000.0,6.654e+14,'TRMMMWA128F426B589'
# 0.2944,3355.0,10780.0,39860000.0,130200000.0,424700000.0,8.751e+11,1159000000.0,3786000000.0,3.047e+13,0.4853,5745.0,19090.0,-44210000.0,-145700000.0,-479700000.0,7.663e+11,1291000000.0,4254000000.0,2.718e+13,'TRMMMXN128F42936A5'
# 1.634,6718.0,46780.0,160900000.0,1126000000.0,7870000000.0,7.106e+12,10060000000.0,70350000000.0,2.436e+15,5.212,11600.0,79840.0,-179500000.0,-1240000000.0,-8555000000.0,6.256e+12,11060000000.0,76400000000.0,2.078e+15,'TRMMMLR128F1494097'
# 1.007,3359.0,22500.0,40040000.0,266200000.0,1769000000.0,8.813e+11,2367000000.0,15740000000.0,2.558e+14,2.634,5770.0,38150.0,-44530000.0,-295500000.0,-1959000000.0,7.738e+11,2622000000.0,17400000000.0,2.265e+14,'TRMMMBB12903CB7D21'
# 1.309,6735.0,35260.0,161600000.0,854800000.0,4518000000.0,7.153e+12,7702000000.0,40720000000.0,1.062e+15,2.559,11620.0,61960.0,-180300000.0,-957200000.0,-5077000000.0,6.297e+12,8620000000.0,45750000000.0,9.372e+14,'TRMMMHY12903CB53F1'
# 0.5483,3355.0,18410.0,40140000.0,222400000.0,1231000000.0,8.851e+11,1993000000.0,11040000000.0,1.508e+14,0.7773,5794.0,31300.0,-44780000.0,-242800000.0,-1316000000.0,7.793e+11,2173000000.0,11790000000.0,1.262e+14,'TRMMMML128F4280EE9'

hdfs dfs -cat /data/msd/audio/statistics/sample_properties.csv.gz | gunzip | head

# track_id,title,artist_name,duration,7digita_Id,sample_bitrate,sample_length,sample_rate,sample_mode,sample_version,filesize
# TRMMMYQ128F932D901,"Silent Night","Faster Pussy cat",252.05506,7032331,128,60.1935770567,22050,1,2,960887
# TRMMMKD128F425225D,"Tanssi vaan",Karkkiautomaatti,156.55138,1514808,64,30.2244270016,22050,1,2,242038
# TRMMMRX128F93187D9,"No One Could Ever","Hudson Mohawke",138.97098,6945353,128,60.1935770567,22050,1,2,960887
# TRMMMCH128F425532C,"Si Vos Querés","Yerba Brava",145.05751,2168257,64,30.2083516484,22050,1,2,240534
# TRMMMWA128F426B589,"Tangle Of Aspens","Der Mystic",514.29832,2264873,64,60.3382103611,22050,1,2,480443
# TRMMMXN128F42936A5,"Symphony No. 1 G minor ""Sinfonie Serieuse""/Allegro con energia","David Montgomery",816.53506,3360982,128,30.1360348456,44100,0,1,481070
# TRMMMLR128F1494097,"We Have Got Love","Sasha / Turbulence",212.37506,552626,64,60.3542857143,22050,1,2,480686
# TRMMMBB12903CB7D21,"2 Da Beat Ch'yall","Kris Kross",221.20444,6435649,128,30.1360348456,44100,0,1,481070
# TRMMMHY12903CB53F1,Goodbye,"Joseph Locke",139.17995,8376489,128,60.2459472422,22050,1,2,961723

hdfs dfs -du -h /data/msd/genre

# 11.1 M  88.7 M  /data/msd/genre/msd-MAGD-genreAssignment.tsv
# 8.4 M   67.3 M  /data/msd/genre/msd-MASD-styleAssignment.tsv
# 10.6 M  85.0 M  /data/msd/genre/msd-topMAGD-genreAssignment.tsv

hdfs dfs -cat /data/msd/genre/msd-MAGD-genreAssignment.tsv | head

# TRAAAAK128F9318786      Pop_Rock
# TRAAAAV128F421A322      Pop_Rock
# TRAAAAW128F429D538      Rap
# TRAAABD128F429CF47      Pop_Rock
# TRAAACV128F423E09E      Pop_Rock
# TRAAADT12903CCC339      Easy_Listening
# TRAAAED128E0783FAB      Vocal
# TRAAAEF128F4273421      Pop_Rock
# TRAAAEM128F93347B9      Electronic
# TRAAAFD128F92F423A      Pop_Rock

hdfs dfs -du -h /data/msd/main

# 174.4 M  1.4 G  /data/msd/main/summary

hdfs dfs -cat /data/msd/main/summary/analysis.csv.gz | gunzip | head

# analysis_sample_rate,audio_md5,danceability,duration,end_of_fade_in,energy,idx_bars_confidence,idx_bars_start,idx_beats_confidence,idx_beats_start,idx_sections_confidence,idx_sections_start,idx_segments_confidence,idx_segments_loudness_max,idx_segments_loudness_max_time,idx_segments_loudness_start,idx_segments_pitches,idx_segments_start,idx_segments_timbre,idx_tatums_confidence,idx_tatums_start,key,key_confidence,loudness,mode,mode_confidence,start_of_fade_out,tempo,time_signature,time_signature_confidence,track_id
# 22050,aee9820911781c734e7694c5432990ca,0.0,252.05506,2.049,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0.777,-4.829,0,0.688,236.635,87.002,4,0.94,TRMMMYQ128F932D901
# 22050,ed222d07c83bac7689d52753610a513a,0.0,156.55138,0.258,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,9,0.808,-10.555,1,0.355,148.66,150.778,1,0.0,TRMMMKD128F425225D
# 22050,96c7104889a128fef84fa469d60e380c,0.0,138.97098,0.0,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,7,0.418,-2.06,1,0.566,138.971,177.768,4,0.446,TRMMMRX128F93187D9
# 22050,0f7da84b6b583e3846c7e022fb3a92a2,0.0,145.05751,0.0,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,7,0.125,-4.654,1,0.451,138.687,87.433,4,0.0,TRMMMCH128F425532C
# 22050,228dd6392ad8001b0281f533f34c72fd,0.0,514.29832,0.0,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5,0.097,-7.806,0,0.29,506.717,140.035,4,0.315,TRMMMWA128F426B589
# 22050,001717032b7105ee4de70f67bf7fba3e,0.0,816.53506,2.194,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0.419,-21.42,1,0.581,811.799,90.689,4,0.158,TRMMMXN128F42936A5
# 22050,eb4f5cb71ff1e50a279d1872929cf6b4,0.0,212.37506,0.0,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0.611,-4.931,0,0.627,206.629,101.45,1,0.96,TRMMMLR128F1494097
# 22050,c5eaa27a919d3403cb12745056de0b21,0.0,221.20444,0.165,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,11,0.332,-12.214,0,0.35,212.12,98.02,4,0.982,TRMMMBB12903CB7D21
# 22050,801588bb617e8dfb92df0465619f6a2c,0.0,139.17995,0.171,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5,0.537,-10.705,1,0.523,130.479,115.427,1,0.324,TRMMMHY12903CB53F1

hdfs dfs -cat /data/msd/main/summary/metadata.csv.gz | gunzip | head

# analyzer_version,artist_7digitalid,artist_familiarity,artist_hotttnesss,artist_id,artist_latitude,artist_location,artist_longitude,artist_mbid,artist_name,artist_playmeid,genre,idx_artist_terms,idx_similar_artists,release,release_7digitalid,song_hotttnesss,song_id,title,track_7digitalid
# ,4069,0.6498221002008776,0.3940318927141434,ARYZTJS1187B98C555,,,,357ff05d-848a-44cf-b608-cb34b5701ae5,Faster Pussy cat,44895,,0,0,Monster Ballads X-Mas,633681,0.5428987432910862,SOQMMHC12AB0180CB8,Silent Night,7032331
# ,113480,0.4396039666767154,0.3569921077564064,ARMVN3U1187FB3A1EB,,,,8d7ef530-a6fd-4f8f-b2e2-74aec765e0f9,Karkkiautomaatti,-1,,0,0,Karkuteillä,145266,0.2998774882739778,SOVFVAK12A8C1350D9,Tanssi vaan,1514808
# ,63531,0.6436805720579895,0.4375038365946544,ARGEKB01187FB50750,55.8578,"Glasgow, Scotland",-4.24251,3d403d44-36ce-465c-ad43-ae877e65adc4,Hudson Mohawke,-1,,0,0,Butter,625706,0.6178709693948196,SOGTUKN12AB017F4F1,No One Could Ever,6945353
# ,65051,0.44850115965646636,0.37234906851712235,ARNWYLR1187B9B2F9C,,,,12be7648-7094-495f-90e6-df4189d68615,Yerba Brava,34000,,0,0,De Culo,199368,,SOBNYVR12A8C13558C,Si Vos Querés,2168257
# ,158279,0.0,0.0,AREQDTE1269FB37231,,,,,Der Mystic,-1,,0,0,Rene Ablaze Presents Winter Sessions,209038,,SOHSBXH12A8C13B0DF,Tangle Of Aspens,2264873
# ,219281,0.361286979627774,0.10962584705877759,AR2NS5Y1187FB5879D,,,,d087b377-bab7-46c4-bd12-15debebb5d61,David Montgomery,-1,,0,0,Berwald: Symphonies Nos. 1/2/3/4,299244,,SOZVAPQ12A8C13B63C,"Symphony No. 1 G minor ""Sinfonie Serieuse""/Allegro con energia",3360982
# ,3736,0.6929227305760355,0.453731585998959,ARO41T51187FB397AB,,"Mexico City, Mexico",,d2461c0a-5575-4425-a225-fce0180de3fd,Sasha / Turbulence,1396,,0,0,Strictly The Best Vol. 34,52968,,SOQVRHI12A6D4FB2D7,We Have Got Love,552626
# ,49941,0.5881561876748532,0.40109242517712895,AR3Z9WY1187FB4CDC2,,,,bf61e8ff-7621-4655-8ebd-68210645c5e9,Kris Kross,9594,,0,0,Da Bomb,580432,,SOEYRFT12AB018936C,2 Da Beat Ch'yall,6435649
# ,15202,0.4084654634687691,0.28590119604546194,ARA04401187B991E6E,54.99241,"Londonderry, Northern Ireland",-7.31923,1a9bf859-1dc2-495b-9e7c-289be7731a9f,Joseph Locke,61524,,0,0,Danny Boy,756677,,SOPMIYT12A6D4F851E,Goodbye,8376489

hdfs dfs -du -h /data/msd/tasteprofile

# 2.0 M    16.2 M  /data/msd/tasteprofile/mismatches
# 488.4 M  3.8 G   /data/msd/tasteprofile/triplets.tsv

hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt | head

# 9d8
# < ERROR: <SOFQHZM12A8C142342 TRMWMFG128F92FFEF2> Josipa Lisac  -  razloga  !=  Lisac Josipa  -  1000 razloga
# 19d17
# < ERROR: <SODXUTF12AB018A3DA TRMWPCD12903CCE5ED> Lutan Fyah  -  Nuh Matter the Crisis Feat. Midnite  !=  Midnite  -  Nah Matter the Crisis
# 29d26
# < ERROR: <SOASCRF12A8C1372E6 TRMHIPJ128F426A2E2> Gaetano Donizetti  -  L'Elisir d'Amore: Act Two: Come sen va contento!  !=  Gianandrea Gavazzeni_ Orchestra E Coro Del Maggio Musicale Fiorentino_ Carlo Bergonzi_ Renata Scotto  -  L'Elisir D'Amore_ Act 2: Come Sen Va Contento (Adina) (Donizetti)
# 33d29
# < ERROR: <SOITDUN12A58A7AACA TRMHXGK128F42446AB> C.J. Chenier  -  Ay, Ai Ai  !=  Clifton Chenier  -  Ay_ Ai Ai
# 52d47
# < ERROR: <SOLZXUM12AB018BE39 TRMRSOF12903CCF516> 許志安  -  男人最痛  !=  Andy Hui  -  Nan Ren Zui Tong


hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_mismatches.txt | head

# ERROR: <SOUMNSI12AB0182807 TRMMGKQ128F9325E10> Digital Underground  -  The Way We Swing  !=  Linkwood  -  Whats up with the Underground
# ERROR: <SOCMRBE12AB018C546 TRMMREB12903CEB1B1> Jimmy Reed  -  The Sun Is Shining (Digitally Remastered)  !=  Slim Harpo  -  I Got Love If You Want It
# ERROR: <SOLPHZY12AC468ABA8 TRMMBOC12903CEB46E> Africa HiTech  -  Footstep  !=  Marcus Worgull  -  Drumstern (BONUS TRACK)
# ERROR: <SONGHTM12A8C1374EF TRMMITP128F425D8D0> Death in Vegas  -  Anita Berber  !=  Valen Hsu  -  Shi Yi
# ERROR: <SONGXCA12A8C13E82E TRMMAYZ128F429ECE6> Grupo Exterminador  -  El Triunfador  !=  I Ribelli  -  Lei M'Ama
# ERROR: <SOMBCRC12A67ADA435 TRMMNVU128EF343EED> Fading Friend  -  Get us out!  !=  Masterboy  -  Feel The Heat 2000
# ERROR: <SOTDWDK12A8C13617B TRMMNCZ128F426FF0E> Daevid Allen  -  Past Lives  !=  Bhimsen Joshi  -  Raga - Shuddha Sarang_ Aalap
# ERROR: <SOEBURP12AB018C2FB TRMMPBS12903CE90E1> Cristian Paduraru  -  Born Again  !=  Yespiring  -  Journey Stages
# ERROR: <SOSRJHS12A6D4FDAA3 TRMWMEL128F421DA68> Jeff Mills  -  Basic Human Design  !=  M&T  -  Drumsettester
# ERROR: <SOIYAAQ12A6D4F954A TRMWHRI128F147EA8E> Excepter  -  OG  !=  The Fevers  -  Não Tenho Nada (Natchs Scheint Die Sonne)

hdfs dfs -cat /data/msd/tasteprofile/triplets.tsv/part-00000.tsv.gz | gunzip | head

# b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOAKIMP12A8C130995      1
# b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOAPDEY12A81C210A9      1
# b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBBMDR12A8C13253B      2
# b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBFNSP12AF72A0E22      1
# b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBFOVM12A58A7D494      1
# b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBNZDC12A6D4FC103      1
# b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBSUJE12A6D4F8CF5      2
# b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBVFZR12A6D4F8AE3      1
# b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBXALG12A8C13C108      1
# b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBXHDL12A81C204C0      1



# -----------------------------------------------------------------------------
# Processing Q1 (b)
# -----------------------------------------------------------------------------

# Check number of blocks, block sizes and replication factor

hdfs fsck /data/msd/audio/features -files -blocks

hdfs fsck /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv

hdfs fsck /data/msd/tasteprofile/triplets.tsv -files -blocks


# -----------------------------------------------------------------------------
# Processing Q1 (c)
# -----------------------------------------------------------------------------

# Count each directory in hdfs

hdfs dfs -cat /data/msd/audio/attributes/* | wc -l # 3929

hdfs dfs -cat /data/msd/audio/features/*/* |gunzip | wc -l # 12927867

hdfs dfs -cat /data/msd/audio/statistics/* | gunzip | wc -l # 992866

hdfs dfs -cat /data/msd/genre/* | wc -l # 1103077

hdfs dfs -cat /data/msd/main/summary/* |gunzip | wc -l # 2000002

hdfs dfs -cat /data/msd/tasteprofile/mismatches/* | wc -l # 20032

hdfs dfs -cat /data/msd/tasteprofile/triplets.tsv/* | gunzip | wc -l # 48373586