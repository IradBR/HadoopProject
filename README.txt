Names and ids:
    - Irad Biton Regev - 314884354
    - May Elnathan - 318785813


Run the project:
    assuming the jars of steps 1-3 in different folders S3 in /jars/stepI/aws_hadoop_2.jar (I is the number of the step)
    run java -jar jarName.jar (args for the actual program)

Our project structure - map-reduce steps:
    Step 1 - Input - Google 3-Gram English dataset.
                   <key, value> - <lineId , n-gram /t year /t occurrences /t pages /t books >
             Map - for each record (trigram - w1 w2 w3, number - occurrences) creates a key value pair:
                   <key, value> -   <<w1, w2, w3>, <occurrences, section>>

             Combiner - to reduce the number of key-value pair sent in Hadoop's network we gather occurrences
                        info from the same 3-gram and send summary to the reducer.
                   <key, value> -   <<w1, w2, w3>, <sum occurrences, section>>

             Reducer - sums the occurrences for each key value pair.
                       when the reducer gets the key "w1 w2 w3" (3-gram) => he sum and produce 3 key values (at most 3):
                       <<r B> , <w1,w2,w3> > => r := the total occurrences of that 3-gram in the corpus (B for future sort)
                                                B := "B"
                       <<r0 A> , 0, r1> => r0 := the value of total occurrences of that 3-gram in the section 0 of the corpus.
                                           A := "A"
                                           0 := (value[0]) - represent the section that the r came from.
                                           r1 := (value[1]) - the value of total occurrences of that 3-gram in the section 1 of the corpus
                                                 (the opposite part of the corpus).

                       <<r1 A> , 1, r0> => Inverted symmetry of the previous key value (A for future sort).

    Step 2 - Input - Output of step 1.
             Map - the mapper send to the reducer all key-value pairs he gets:
              <key, value> -   <<r B>, <w1,w2,w3> >
                               <<r0 A> , <0, r1 , 1>>
                               <<r1 A> , <1, r0 , 1>>

             Combiner - to reduce the number of key-value pair sent in Hadoop's network we gather occurrences
                                     info from the same keys and send summary to the reducer.
             <key, value> -   <<r B>, <w1,w2,w3> >
                              <<r0 A> , <0, sum_r1 , sum_occurrences_section>>
                              <<r1 A> , <1, sum_r0 , sum_occurrences_section>>

             Partition - we take care that all the <r0 A> , <r1 A> and <r B> for the same r will go to the same computer.

             Reduce - because of the partition we know that for a specific r we first get all the <r0 A> , <r1 A> for the calculation
                      of : Nr0 , Nr1 , Tr0 , Tr1 and then we can calculate P(r) -
                        Nr0 => for all values where value[0] = section 0 : we sum all of (value[2]) - sum_occurrences_section
                                and gets the number of 3grams types occurring r times in section 0 of the corpus.
                        Nr1 => for all values where value[0] = section 1 : we sum all of (value[2]) - sum_occurrences_section
                                and gets the number of 3grams types occurring r times in section 1 of the corpus.
                        Tr0 => for all values where value[0] = section 0 : we sum all of (value[1]) - sum_r1
                                and gets the total number of 3grams of section 0 (of Nr0) appear in section 1.
                        Tr1 => for all values where value[0] = section 1 : we sum all of (value[1]) - sum_r0
                                and gets the total number of 3grams of section 1 (of Nr1) appear in section 0.
                        N => is saved in advance in a static variable - representing the num of 3grams after the filtered by "stop words".
                      when we finish pass all the values of <r0/1 A> we calculate the P(r) = (Tr0 + Tr1)/(N * (Nr0 + Nr1)) according to the formula.
                      after we calculate P(r) we save his value in static variable.
                      Then we will get all thw 3grams with the same r (<r B>, <w1,w2,w3>) and create:
                      <key, value> - < <w1,w2,w3> , P(r) > // for each 3gram

    Step 3 - Input - Output of step 2.
             Map - for each <key, value> - < <w1,w2,w3> , P(r) > we change the order of the data like this :
                <key, value> - <<w1 w2 p(r)>, w3> - the purpose is to get the right order of the output by preparing the key to the Comparator.
             Comparator - first we compare w1 w2 - ascending -> second we compare by probability for w1 w2 w3 - descending.
             Reducer - gets the <key, value> sorted by the comparator and change the order of the data to the right order :
                <key, value> - < <w1,w2,w3> , P(r) > // for each 3gram
                * we set : job.setNumReduceTasks(1) => the purpose is to get all the data in 1 sorted output.

Statistics:
We compare the statistics data - with combiner and without combiner.
We see a huge change - the number of key-value pairs passing through the network is drastically lower when using the combiner.

The statistics data:
without combiner:
- step 1:
	- map input records: 163,471,963
	- map output records: 14,509,393
	- map output bytes: 475,799,832
	- map output materialized bytes: 62,648,769
	- combine input records: 0
	- combine output records: 0 
	- reduce input records: 14,509,393
	- reduce shuffle bytes: 62,648,769
	- reduce output records: 1,103,280

- step 2:
	- map input records: 1,103,280
	- map output records: 1,103,280
	- map output bytes: 22,351,330
	- map output materialized bytes: 9,997,044
	- combine input records: 0
	- combine output records: 0
	- reduce input records: 1,103,280
	- reduce shuffle bytes: 9,997,044
	- reduce output records: 367,762

step 3 doesn't use combiner - every key-value is unique. His Statistics data:
	- map input records: 367,762
	- map output records: 367,762
	- map output bytes: 18,530,044
	- map output materialized bytes: 7,866,690
	- combine input records: 0
	- combine output records: 0
	- reduce input records: 367,762
	- reduce shuffle bytes: 7,866,690
	- reduce output records: 367,762


with combiner:
- step 1:
	- map input records: 163,471,963
	- map output records: 14,509,393
	- map output bytes: 475,799,832
	- map output materialized bytes: 10,875,428
	- combine input records: 14,509,393
	- combine output records: 735,519
	- reduce input records: 735,519
	- reduce shuffle bytes: 10,875,428
	- reduce output records: 1,103,280

- step 2:
	- map input records: 1,103,280
	- map output records: 1,103,280
	- map output bytes: 22,351,330
	- map output materialized bytes: 7,170,591
	- combine input records: 1,103,280
	- combine output records: 379,511
	- reduce input records: 379,511
	- reduce shuffle bytes: 759,022
	- reduce output records: 367,762

(step 3 doesn't use combiner - every key-value is unique)
Same data as without the combiner.

Analysis:
"interesting" word 
For same 3-grams (usually with big "r") -  there were no 3-grams in section 0 and in section 1 with the same "r"  - so Nr0 and Nr1 is "0" and the denominator in the formula is "0".
Therefore, words that appeared frequently can have a probability of 0.
This made us realize that there is a problem with the formula (Meni agreed with us on this) -You can see this in examples below.

1. 
אב בית די	2.3297578869200184E-6
אב בית דין	0.0
אב בית הדין	0.0
אב בפני עצמו	1.4902100249095782E-6
אב בת קמה	9.422288882820002E-7
אב בתורה אב	1.1755461100771232E-6
It is not possible that a word with a spelling error has a greater probability than a correct word.


2.
רבים מבני הנוער	1.6404885263433767E-5
רבים מבני עמנו	1.412438551829275E-5
רבים מבני הדור	1.0125151361124632E-5
רבים מבני דורו	9.999040776139876E-6
רבים מבני הקהילה	8.991820312192706E-6
רבים מבני העיר	6.271208014083755E-6
רבים מבני העדה	4.908037137110457E-6
רבים מבני העם	3.669147667787398E-6
רבים מבני הנעורים	3.3630024149391375E-6
רבים מבני משפחתו	2.907827820569101E-6
רבים מבני המשפחה	2.670656325190107E-6
רבים מבני עמינו	2.670656325190107E-6
רבים מבני עדות	2.6580498229156294E-6
Probability seems to make sense for how often words are used.


3.
לבית המשפט המחוזי	1.3254986788473595E-5
לבית המשפט הגבוה	7.038978372855649E-6
לבית המשפט כדי	2.4762163243290855E-6
לבית המשפט סמכות	2.1437524499264655E-6
לבית המשפט האזרחי	1.5340679796138033E-6
Probability seems to make sense for how often words are used.


3.
אני אעשה הכל	1.962534878735652E-6
אני אעשה הכול	1.8429388683553137E-6
Sentences with the same meaning (full and missing spelling) with similar probability.

4.
אינו יכול להתעלות	8.628412361890945E-7
אינו יכול להתרומם	8.628412361890945E-7
אינו יכול להתיישב	8.450413766347919E-7
אינו יכול לאמור	8.280217593454838E-7
אינו יכול להתנער	8.0236180016185E-7
אינו יכול לומר	0.0
אינו יכול לחזור	0.0
אינו יכול לקבל	0.0
אינו יכול לעמוד	0.0
אינו יכול שלא	0.0
אינו יכול לשמש	0.0
Frequently used words, their probability cannot be 0 (a problem with the formula).

5.
אוניברסיטת בךגוריון בנגב	3.2672584656316202E-6
אוניברסיטת בן גוריון	1.1700259829141316E-5
אוניברסיטת בר אילן	2.5957762076029088E-5
אוניברסיטת חיפה אוניברסיטת	2.828837601368815E-6
אוניברסיטת חיפה /	1.846125268959321E-6
אוניברסיטת חיפה וזמורה	1.8054450824812057E-6
אוניברסיטת חיפה החוג	1.2346379998757767E-6
אוניברסיטת ניו יורק	1.4342395237004078E-6
אוניברסיטת קולומביה בניו	8.713197252007128E-7
Probability seems to make sense for how often words are used.

6.
אולי משום שלא	1.267201194788497E-5
אולי משום שהוא	6.400597228687882E-6
אולי משום שהיה	5.3012423511040175E-6
אולי משום שהיא	3.3769264502038984E-6
אולי משום שאין	3.0592178057100368E-6
אולי משום שהם	3.0361492079480716E-6
אולי משום שאני	2.4762163243290855E-6
Probability seems to make sense for how often words are used.

7.
אלף והמש מאות	4.212723076032666E-6
אלף וחמש מאות	0.0
אלף וכמה מאות	9.195880743110286E-7
אלף ומאה כסף	2.120105884161047E-6
אלף ומאה הכסף	1.6991668566829108E-6
It is not possible that a word with a spelling error has a greater probability than a correct word.

8.
אני אוהב מאוד	5.8141571940171165E-6
אני אוהב אתכם	5.6923165297494554E-6
אני אוהב לראות	2.7021499733928965E-6
אני אוהב מאד	2.5660051449131834E-6
אני אוהב לשמוע	2.2619567183695253E-6
אני אוהב לקרוא	1.905816631999867E-6
אני אוהב גם	1.670820865360278E-6
אני אוהב לעבוד	1.3340112849527798E-6
אני אוהב אנשים	1.3029939961966706E-6
אני אוהב אותן	1.2346379998757767E-6
Probability seems to make sense for how often words are used.

9.
תרבות יהודית חילונית	3.843336846470631E-6
תרבות יהודית מודרנית	3.715416690661188E-6
תרבות יהודית חדשה	2.6141042792873602E-6
תרבות יהודית בברית	2.4022387971341485E-6
תרבות יהודית לאומית	2.2294782157548566E-6
תרבות יהודית מקורית	1.905816631999867E-6
תרבות יהודית וכללית	1.1755461100771232E-6
תרבות יהודית שורשית	8.713197252007128E-7
Probability seems to make sense for how often words are used.

10.
תשומת הלב לכך	7.907229216866203E-6
תשומת הלב הציבורית	6.7988891438297166E-6
תשומת הלב הראויה	5.557664152235208E-6
תשומת הלב לעובדה	4.290912654106347E-6
תשומת הלב גם	2.2238272256475446E-6
תשומת הלב הדרושה	2.2238272256475446E-
Probability seems to make sense for how often words are used.

The full output in file "finalOutpuStep3".