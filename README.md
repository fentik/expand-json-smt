# Kafka connect SMT to expand JSON string

This java lib implements Kafka connect SMT (Single Message Transformation) that flattens a JSON array field (targetField) into N distinct
fields with the value of spliceField as a suffix and set the value to value of outputField in the JSON object.

## Config

Use it in connector config file like this:
~~~json
...
"transforms": "splitjsonarray",
"transforms.splitjsonarray.type": "com.fentik.dataflo.expandjsonsmt.SplitJSONArray$Value",
"transforms.splitjsonarray.targetField": "metadata"
"transforms.splitjsonarray.spliceField": "splice"
"transforms.splitjsonarray.outputField": "field"
"transforms.splitjsonarray.outputFieldType"" "type"
...
~~~

## Example:

If the Kafka message value is of the form:

{
 team_id: 1
 people_locations = [
		     { "person_id": 1,
		       "address" = {
		          'city': 'San Mateo',
 		          'country': 'US',
		       }
	             },
		    { "person_id": 2,
		      "address" = {
		         'city': 'San Francisco',
 			 'country': 'US',
		       }
		    },
		]
},

and the transformer config is

{
  targetField: person_locations
  newTargetFieldPrefix: person_location
  spliceField: person_id
  outputField: address
  outputType: json
},

the output will be:

{
 team_id: 1
 people_location_1 = {'city': 'San Mateo',
                      'country': 'US'
		      }
 people_location_2 = {'city': 'San Francisco',
                      'country': 'US'
		      }
}

## Build
mvn package

## Installation
After build copy file `target/kafka-connect-smt-splitjsonarraysmt-0.0.7-assemble-all.jar`

KAFKA_CONNECT_PLUGINS_DIR=/opt/kafka/connect/.
cp ./target/target/kafka-connect-smt-splitjsonarraysmt-0.0.7-assemble-all.jar $KAFKA_CONNECT_PLUGINS_DIR

