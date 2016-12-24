
# Import needed libaries

import xml.etree.cElementTree as ET
import pprint
import re
from collections import defaultdict
import csv
import codecs
import cerberus
import schema
import random

"""
The structure of this file is as follows:

1. OSM File preparation (select full of sample set of OSM data)
2. Data exploration
3. Data auditing
4. Data fixing (based on audit results)
5. Data shaping, validating and exporting to .csv format (for SQL)

"""

# 1. OSM FILE PREPARATION SECTION:
print "FILE BEING USED FOR THIS COMPILATION:"
print "-------------------------------------"

"""
OSM data for the entire island of Oahu has been downloaded from:
https://mapzen.com/data/metro-extracts/metro/honolulu_hawaii/
The file is slightly larger than 54MB when uncompressed.

The 'SAMPLE_FILE' is meant to be a much smaller file to allow for
faster testing / debugging cycles. The size of the file can be
adjusted by adjusting the value of 'k'. Larger values of k yield
smaller 'SAMPLE_FILE' sizes.
"""

OSM_FILE = "honolulu.osm"
SAMPLE_FILE = "sample.osm"

# The get_element function was provided by Udacity and used unmodified

k = 10 # Parameter: take every k-th top level element

def get_element(osm_file, tags=('node', 'way', 'relation')):

    """
    Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

with open(SAMPLE_FILE, 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')

    # Write every kth top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % k == 0:
            output.write(ET.tostring(element, encoding='utf-8'))

    output.write('</osm>')

# Choose the File to Use for the remainder of the code:
# Uncomment to "honolulu.osm" if ready to run on full set or use "sample.osm" to run on sample set

#USE_FILE = OSM_FILE
USE_FILE = SAMPLE_FILE

print USE_FILE
if USE_FILE == "sample.osm":
    print "(takes every k-th level element from full OSM file, where k equals:", k
else:
    print "(this is the complete OSM file for Honolulu)"
print '\n'

# 2. DATA EXPLORATION SECTION:
print "DATA EXPLORATION:"
print "-----------------"

#
# Iterative parsing to generate a dictionary of the various node names:
# Code adapted from: Udacity OSM SQL Case Study Slide 3
#

def count_tags(filename):

    tags={}

    for event, elem in ET.iterparse(filename, events=("start",)):

        if elem.tag in tags.keys():
            tags[elem.tag] += 1
        else:
            tags[elem.tag] = 1

    return tags

print "Tag Type Counts for File:"
print "\n"
print count_tags(USE_FILE)
print '\n'

#
# Iterative parsing to generate a dictionary of the count of 4 tag categories:
# Code adapted from: Udacity OSM SQL Case Study Slide 6
#

"""
four tag categories were provided / defined by Udacity case study:

  "lower", for tags that contain only lowercase letters and are valid,
  "lower_colon", for otherwise valid tags with a colon in their names,
  "problemchars", for tags with problematic characters, and
  "other", for other tags that do not fall into the other three categories.
"""

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

def key_type(element, keys):

    n = 0
    if element.tag == "tag":

        k = element.attrib['k']

        if re.search(lower,k):
            keys["lower"] += 1
        elif re.search(lower_colon,k):
            keys["lower_colon"] += 1
        elif re.search(problemchars,k):
            keys["problemchars"] += 1
        else:
            keys["other"] += 1

# Random number allows a certain percent of 'other' tags to be printed:

            if random.randint(1,100) <= 2:
                print k

    return keys

def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}

    print "Sample 'other' tags, randomly (2%) selected:"
    print "\n"

    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    print "\n"
    print "Count of the four Tag Categories:"
    print "\n"
    print keys
    return keys

process_map(USE_FILE)
print '\n'

#
# Iterative parsing to generate a count of the number of unique contributng users:
# Code adapted from: Udacity OSM SQL Case Study Slide 7
#

def get_user(element):
    if element.get('uid'):
        uid = element.attrib['uid']
        return uid

def process_map(filename):
    users = set()
    for _, element in ET.iterparse(filename):

        if get_user(element):
            users.add(get_user(element))

    return users

users = process_map(USE_FILE)

print "Number of Unique User IDs:"
print "\n"
print len(users)
print "\n"


# 3. DATA AUDITING SECTION:
print "DATA AUDITING:"
print "--------------"

# Auditing steps to identify problematic streets, zips and state values:
# Code adapted from: Udacity OSM SQL Case Study Slide 10

# (a) Check for problematic street type names:

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

# expected provides a list of expected street types; list adjusted based on initial audit results
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons", "Circle", "Highway", "Loop", "Terrace", "Way", "Mall"]

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)

def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

def audit_street(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types

st_types = audit_street(USE_FILE)
print "Possible problematic street types:"
print "\n"
pprint.pprint(dict(st_types))
print "\n"

# (b) Check for problematic zip codes:

def is_zip_code(elem):
    return (elem.attrib['k'] == "addr:postcode")

def audit_zip(osmfile):
    osm_file = open(osmfile, "r")
    prob_zip = set()
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_zip_code(tag):

                    if len(tag.attrib['v']) != 5:
                        prob_zip.add(tag.attrib['v'])
                    elif tag.attrib['v'][0:2] != '96':
                        prob_zip.add(tag.attrib['v'])
    osm_file.close()
    return prob_zip

print "Possible problematic zip codes:"
print "\n"
print audit_zip(USE_FILE)

# (c) Check for problematic state values (all should equal 'HI'):

def is_state(elem):
    return (elem.attrib['k'] == "addr:state")

def audit_state(osmfile):
    osm_file = open(osmfile, "r")
    prob_state = set()
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_state(tag):

                    if tag.attrib['v'] != 'HI':
                        prob_state.add(tag.attrib['v'])
    osm_file.close()
    return prob_state

print "\n"
print "Possible problematic state values:"
print "\n"
print audit_state(USE_FILE)
print "\n"


print "DATA FIXING & SHAPING:"
print "----------------------"

# Steps for fixing problematic data and shaping data for export to .csv:
# Shaping / exporting code adapted from: Udacity OSM SQL Case Study Slide 11

# Because the Udacity code was so crucial for this project, the original case study commentary
# is left here in its entirety, for reference purposes:

# UDACITY COMMENTARY STARTS:
"""
After auditing is complete the next step is to prepare the data to be inserted into a SQL database.
To do so you will parse the elements in the OSM XML file, transforming them from document format to
tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
imported to a SQL database as tables.

The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

We've already provided the code needed to load the data, perform iterative parsing and write the
output to csv files. Your task is to complete the shape_element function that will transform each
element into the correct format. To make this process easier we've already defined a schema (see
the schema.py file in the last code tab) for the .csv files and the eventual tables. Using the
cerberus library we can validate the output against this schema to ensure it is correct.

## Shape Element Function
The function should take as input an iterparse Element object and return a dictionary.

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
- timestamp
- changeset
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag should be ignored
- if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
  and characters after the ":" should be set as the tag key
- if there are additional ":" in the "k" value they and they should be ignored and kept as part of
  the tag key. For example:

  <tag k="addr:street:name" v="Lincoln"/>
  should be turned into
  {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}

- If a node has no secondary tags then the "node_tags" field should just contain an empty list.

The final return value for a "node" element should look something like:

{'node': {'id': 757860928,
          'user': 'uboot',
          'uid': 26299,
       'version': '2',
          'lat': 41.9747374,
          'lon': -87.6920102,
          'timestamp': '2010-07-22T16:16:51Z',
      'changeset': 5288876},
 'node_tags': [{'id': 757860928,
                'key': 'amenity',
                'value': 'fast_food',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'cuisine',
                'value': 'sausage',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'name',
                'value': "Shelly's Tasty Freeze",
                'type': 'regular'}]}

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
-  user
- uid
- version
- timestamp
- changeset

All other attributes can be ignored

The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element

The final return value for a "way" element should look something like:

{'way': {'id': 209809850,
         'user': 'chicago-buildings',
         'uid': 674454,
         'version': '1',
         'timestamp': '2013-03-13T15:58:04Z',
         'changeset': 15353317},
 'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
               {'id': 209809850, 'node_id': 2199822390, 'position': 1},
               {'id': 209809850, 'node_id': 2199822392, 'position': 2},
               {'id': 209809850, 'node_id': 2199822369, 'position': 3},
               {'id': 209809850, 'node_id': 2199822370, 'position': 4},
               {'id': 209809850, 'node_id': 2199822284, 'position': 5},
               {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
 'way_tags': [{'id': 209809850,
               'key': 'housenumber',
               'type': 'addr',
               'value': '1412'},
              {'id': 209809850,
               'key': 'street',
               'type': 'addr',
               'value': 'West Lexington St.'},
              {'id': 209809850,
               'key': 'street:name',
               'type': 'addr',
               'value': 'Lexington'},
              {'id': '209809850',
               'key': 'street:prefix',
               'type': 'addr',
               'value': 'West'},
              {'id': 209809850,
               'key': 'street:type',
               'type': 'addr',
               'value': 'Street'},
              {'id': 209809850,
               'key': 'building',
               'type': 'regular',
               'value': 'yes'},
              {'id': 209809850,
               'key': 'levels',
               'type': 'building',
               'value': '1'},
              {'id': 209809850,
               'key': 'building_id',
               'type': 'chicago',
               'value': '366409'}]}
"""
# UDACITY COMMENTARY ENDS

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

# 4. DATA FIXING SECTION:

# Consists of functions for fixing street type, zip code and state name issues, identified
# during the data auditing step

print "Street Types, State Codes and Zip Codes Fixed in the Data:"
print "\n"

def fix_element(elem):

    # Fix Street Names:

    # mapping provides a dictionary for updating potentially problematic street type names.
    # Dictionary contents were updated iteratively, based on the audit results

    mapping = { "St.": "Street",
                "St": "Street",
                "Street.":"Street",
                "Ave": "Avenue",
                "Rd.": "Road",
                "Blvd": "Boulevard",
                "Dr":"Drive",
                "Hwy":"Highway",
                "highway":"Highway",
                "Pkwy": "Parkway"
                }

    def fix_street(elem):

        street_types = defaultdict(set)
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])

                    for st_type, ways in street_types.iteritems():
                        for name in ways:
                                for key,value in mapping.items():
                                    n = street_type_re.search(name)
                                    if n:
                                        street_type = n.group()
                                        if street_type not in expected:
                                                if street_type in mapping:
                                                    better_name = name.replace(key,value)
                                                    if better_name != name:
                                                        print "Fixed Street:", tag.attrib['v'], "=>", better_name
                                                        tag.attrib['v'] = better_name
                                                        return

    # Fix Zip Codes:

    def fix_zip(elem):

            if elem.tag == "node" or elem.tag == "way":
                for tag in elem.iter("tag"):
                    if is_zip_code(tag):
                        if len(tag.attrib['v']) != 5:
                            if tag.attrib['v'][0:2] == '96':
                                print "Fixed Zip:   ", tag.attrib['v'], "=>", tag.attrib['v'][0:5]
                                tag.attrib['v'] = tag.attrib['v'][0:5]
                            else:
                                print "Fixed Zip:   ", tag.attrib['v'], "=>", tag.attrib['v'][-5:]
                                tag.attrib['v'] = tag.attrib['v'][-5:]

    # Fix States:

    def fix_state(elem):

            if elem.tag == "node" or elem.tag == "way":
                for tag in elem.iter("tag"):
                    if is_state(tag):
                        if tag.attrib['v'] != 'HI':
                                print "Fixed State: ", tag.attrib['v'], "=> 'HI'"
                                tag.attrib['v'] = 'HI'

    fix_street(elem)
    fix_zip(elem)
    fix_state(elem)

# 4. DATA SHAPING, VALIDATING AND EXPORTING SECTION:

def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    # Fix data issues, based on auditing results

    fix_element(element)

    # Shape elements, by splitting the attribute fields per the rules described in the
    # Udacity commentary

    if element.tag == 'node':

            for node_field in node_attr_fields:
                node_attribs[node_field] =element.attrib[node_field]

            for tag in element.iter('tag'):
                k = tag.attrib['k']

                # ignores tags containing problem characters in the k tag attribute:

                if re.search(PROBLEMCHARS,k):
                    continue
                else:
                    pass

                tag_dict = {}

                tag_dict['id'] = node_attribs['id']

                colon_find = re.split('[:]', k)

                if len(colon_find) == 1:

                    tag_dict['key'] = k
                    tag_dict['type'] = 'regular'

                elif len(colon_find) == 2:

                    tag_dict['key'] = colon_find[1]
                    tag_dict['type'] = colon_find[0]

                elif len(colon_find) > 2:

                    tag_dict['key'] = ':'.join(colon_find[1:])
                    tag_dict['type'] = colon_find[0]

                tag_dict['value'] = tag.attrib['v']

                tags.append(tag_dict)

            return {'node': node_attribs, 'node_tags': tags}

    elif element.tag == 'way':

        for way_field in way_attr_fields:
            way_attribs[way_field] =element.attrib[way_field]

        for tag in element.iter('tag'):
            k = tag.attrib['k']

            # ignores tags containing problem characters in the k tag attribute:

            if re.search(PROBLEMCHARS,k):
                print "Problem character found - skipping element"
                continue
            else:
                pass

            tag_dict = {}

            tag_dict['id'] = way_attribs['id']

            colon_find = re.split('[:]', k)

            if len(colon_find) == 1:

                tag_dict['key'] = k
                tag_dict['type'] = 'regular'

            elif len(colon_find) == 2:

                tag_dict['key'] = colon_find[1]
                tag_dict['type'] = colon_find[0]

            elif len(colon_find) > 2:

                tag_dict['key'] = ':'.join(colon_find[1:])
                tag_dict['type'] = colon_find[0]

            tag_dict['value'] = tag.attrib['v']

            tags.append(tag_dict)

        n = 0
        for nd in element.iter('nd'):

            nd_dict = {}

            nd_dict['id'] = way_attribs['id']
            nd_dict['node_id'] = nd.attrib['ref']
            nd_dict['position'] = n
            way_nodes.append(nd_dict)
            n+=1

        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}

# ================================================== #
#        Helper Functions: Provided by Udacity       #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)

        raise Exception(message_string.format(field, error_string))

class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

# ================================================== #
#          Main Function: Provided by Udacity        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])

# Note: Validation is ~ 10X slower. For the project consider using a small
# sample of the map when validating.

# Set validation to False to speed up testing and debugging runs

process_map(USE_FILE, validate=True)
print "\n"
print "DATA EXPORTING FOR SQL DATABASE:"
print "--------------------------------"
print "XML to CSV Conversion Completed"