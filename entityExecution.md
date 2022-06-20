# Execution Commands
1. [Description](#desc)
2. [Individual based Commands](#i1)
3. [Family based Commands](#f2)
4. [Execution Output](#op)
5. [Issues/Output](#issue)
    


## Description <a name="desc"></a>
* Docker service is running at 143.169.238.131 Server. Express REST API can be accessed in port 80.
* Added Sample commands to create Individuals/family and perform few updates on them.

## Individual based Commands<a name="i1"></a>

* curl based commands to trigger the specific endpoint of express REST API based on the URL.

  *  **Create Individual**

     *  [ESAT Create Individuals URL](https://wings.esat.kuleuven.be/PhenBook/Individuals)
     *  Arguments : Individual ID and meta data
```gherkin=
  curl --request POST --header 'content-type: application/json' --url http://143.169.238.131:80/addIndividuals --data '{
"Individuals" : [
                    {
                      "IndividualID" : "90002399431",
                      "Meta" : {
                           "IndividualFName" : "fname1",
                           "IndividualLName" : "lname1",
                           "IndividualBirthDate" : "",
                           "IndividualStatus" : "Alive",
                           "IndividualSex" : "M",
                           "Case_Control" : "1",
                           "Affected" : "N"
                           }     
                    } ] }'
```
 *  **Update Individual**

     *  [ESAT Update Individual URL](https://wings.esat.kuleuven.be/PhenBook/Individuals) --> Edit Individual
     *  Arguments : Individual ID and Meta Data to be updated

```gherkin=
curl --request POST --header 'content-type: application/json' --url http://143.169.238.131:80/updateIndividuals --data '{
"Individuals" : [
                    {
                      "IndividualID" : "90002399431",
                      "Meta" : {
                           "IndividualFName" : "fname_upd",
                           "IndividualLName" : "lname_upd"
                           }
                    }
                ] }'
```

 *  **Get Individuals based on Attributes**
 
     *  Arguments : Individual ID and Meta Data to be updated

```gherkin=
curl --request GET http://143.169.238.131:80/getInd/IndividualSex:M-Affected:N 
```

 *  **Get Individual Data based on Individual ID**

```gherkin=
curl --request GET http://143.169.238.131:80/individual/90002399431
```
## Family based Commands<a name="f2"></a>

*  **Create New Family**

     *  [Create Family](https://wings.esat.kuleuven.be/PhenBook/Family) --> Add New Family
     * Arguments : FamilyID,description and other arguments show below.
     * Return Value : JSON Data holding familyID and related attributes

```gherkin=
curl -X POST \
http://143.169.238.131:80/addNewFamily \
-H 'Content-Type: application/json' \
-H 'Host: 143.169.238.131:80' \
-d '{ "_id" : 89710002399436,
"Desc" : "Family Creating from REST API",
"UserID" : 9763,
"PIID" : 56,
"DateAdd" : "2012-11-04T14:51:06.157Z"
}'
```

*  **Get Family Data **

     *  Get Family Data based on PIID or FamilyID
     *  URL Arguments : http://143.169.238.131:80/assignIndividual/FamilyID/IndividualID

**Get Family Data based on FamilyID**
```gherkin=
curl -X GET http://143.169.238.131:80/getFamily/FamilyID/99710002399436

```
**Get Family Data based on PIID**
```gherkin=
curl -X GET http://143.169.238.131:80/getFamily/PIID/56

```
**Get All the families within a particular Center**
```gherkin=
curl -X GET http://143.169.238.131:80/getFamily/PIID/-1

```

*  **Update Family**

     *  [Update Family](https://wings.esat.kuleuven.be/PhenBook/Family) --> Add New Family
     * Arguments : FamilyID and other meta data to be updated.
     * Meta Data : Provide only the fields names that has to be updated

Updating Description and PIID
```gherkin=
curl -X POST http://143.169.238.131:80/updateFamily -H 'Content-Type: application/json' -H 'Host: 143.169.238.131:80' --data '{
"Family" : [
                    {
                      "FamilyID" : 68710002399436,
                      "Meta" : {
                           "Desc" : "Updating family data1",
                           "PIID" : 765
                           }     
                    } ] }'

```
Updating PIID and Description for the given FamilyID
```gherkin=
curl -X POST http://143.169.238.131:80/updateFamily -H 'Content-Type: application/json' -H 'Host: 143.169.238.131:80' --data '{
"Family" : [
                    {
                      "FamilyID" : 68710002399436,
                      "Meta" : {
                           "Desc" : "Updated family data1"
                           }     
                    } ] }'
```
*  **Assign Individual to Family**

     *  [Update Family](https://wings.esat.kuleuven.be/PhenBook/FamilyHealthHistory) --> Assign Individual
     *  URL Arguments : http://143.169.238.131:80/assignIndividual/FamilyID/IndividualID

```gherkin=
curl -X GET http://143.169.238.131:80/assignIndividual/40002399436/90002399431

```

*  **Add Pedigree**

     *  [Update Family](https://wings.esat.kuleuven.be/PhenBook/FamilyHealthHistory) --> Add Family Members
     * Arguments : Pedigree generated by GoJS and the family ID.

```gherkin=
curl -X POST \
  http://143.169.238.131:80/addPedigree \
  -H 'Content-Type: application/json' \
  -H 'Host: 143.169.238.131:80' \
  -d '{
    "_id" : "40002399436",
    "update" : { "pedigree" :     [
      { "key": 0, "n": "Aaron", "s": "M", "m":-10, "f":-11, "ux": 1, "a": ["C", "F", "K"] },
      { "key": 8, "n": "Chloe", "s": "F", "m": 1, "f": 0, "vir": 9, "a": ["E"] } ] }
}'
```

*  **Show Pedigree**

     *  [Update Family](https://wings.esat.kuleuven.be/PhenBook/FamilyHealthHistory) --> Draw Pedigree
     * Arguments : FamilyID

```gherkin=
curl -X GET http://143.169.238.131:80/showPedigree/40002399436
```
## Execution Output <a name="op"></a>


```gherkin=
curl --request POST --header 'content-type: application/json' --url http://143.169.238.131:80/addIndividuals --data '{
> "Individuals" : [
>                     {
>                       "IndividualID" : "90002399431",
>                       "Meta" : {
>                            "IndividualFName" : "fname1",
>                            "IndividualLName" : "lname1",
>                            "IndividualBirthDate" : "",
>                            "IndividualStatus" : "Alive",
>                            "IndividualSex" : "M",
>                            "Case_Control" : "1",
>                            "Affected" : "N"
>                            }     
>                     } ] }'
{"success":true}

curl -X GET http://143.169.238.131:80/showPedigree/40002399436
Individuals corresponding to the added filter{"_id":"40002399436","Desc":"Test family creation from REST API","pedigree":[{"key":0,"n":"Aaron","s":"M","m":-10,"f":-11,"ux":1,"a":["C","F","K"]},{"key":8,"n":"Chloe","s":"F","m":1,"f":0,"vir":9,"a":["E"]}],"relatives":[]}
```

## Output/Issues <a name="issue"></a>
* Expected Output : {"success":true}  or specific details in json format.
* Issues : Please contact me if there is no response or if there are any issues with the argument 
* If IndividualID or FamilyID already exists in DB, try with a different ID

