#!/usr/bin/python3

from enum import Enum
import argparse, sys, json, ldap3

# Map of "Connector IDs" to the underlying connector type.
connectorIds= {'7251f6c8-93c9-11eb-95ba-12704e0d6a4d':'activeScale',
               '7e3f3f5e-350c-4717-891a-2f451c24b0d4':'blackPearl',
               '7c100eae-40fe-11e9-95a3-9cb6d0d9fd63':'box',
               '1b6374b0-f6a4-4cf7-a26f-f262d9c6ca72':'ceph',
               '56366b96-ac98-11e9-abac-9cb6d0d9fd63':'googleGcs',
               '976cf0cf-78c3-4aab-82d2-7c16adbcc281':'googleDrive',
               'e47b6920-ff57-11ea-8aaa-000c297ab3c2':'iRods',
               '28ef55da-1f97-11eb-bdfd-12704e0d6a4d':'oneDrive',
               '145812c8-decc-41f1-83cf-bb2a85a2a70b':'posix',
               '052be037-7dda-4d20-b163-3077314dc3e6':'posixStaging',
               '7643e831-5f6c-4b47-a07f-8ee90f401d23':'s3'}

#
# Parse command line.
#
description = '''Given JSON from stdin from Globus containing a list of 
identities, prints the corresponding POSIX username from an LDAP database.

Note: only POSIX connectors using LDAP authentication are supported.'''
argparser = argparse.ArgumentParser(description=description)

# Args required by Globus
argparser.add_argument('-c', help='Globus Connector ID',
                       default='145812c8-decc-41f1-83cf-bb2a85a2a70b')
argparser.add_argument('-s', help='Globus Storage ID (unused)')
argparser.add_argument('-a', action='store_true',
                       help='''
If set, print all matches to stdout. Otherwise, print only the first match.
                       ''')

# Implementation-specific args.
argparser.add_argument('--no-ssl', action='store_true',
                       help='If set, connect to LDAP unencrypted.')
argparser.add_argument('--admin-dn',
                       help='The DN of the LDAP administrator.')
argparser.add_argument('--password-file',
                       help='Plaintext LDAP password file')
argparser.add_argument('--host', default='localhost',
                       help='Hostname of the LDAP server')
argparser.add_argument('--port', type=int, default=389,
                       help='Port number of the LDAP server')
args = argparser.parse_args()

connectorType = connectorIds.get(args.c, '*unknown*')
if connectorType != 'posix':
    raise ValueError(("Connector type with id %s ('%s') not supported" %
                      (args.c, connectorType)))

# Parse and validate input.
jsonObj = json.load(sys.stdin)
dataType = jsonObj.get('DATA_TYPE')
if dataType != "identity_mapping_input#1.0.0":
    raise ValueError(('''
DATA_TYPE "%s" is unsupported, must be "identity_mapping_input#1.0.0"
''' % dataType))
identities = jsonObj.get('identities')
if identities == None or len(identities) < 1:
    raise ValueError('Input JSON contains no identities')

#
# Connect to LDAP
#
ldapPassword = ''
with open(args.password_file, 'r') as fileHandle:
    ldapPassword = fileHandle.readlines()[0].replace('\n', '')

# TODO: Make this more useful, e
ldapServer = ldap3.Server(host=args.host,
                          port=args.port,
                          use_ssl=False, get_info='ALL')
ldapConnection = ldap3.Connection(ldapServer, user=args.admin_dn,
                                  password=ldapPassword)
ldapConnection.open()
ldapConnection.bind()
if not ldapConnection.bound:
    raise RuntimeError("Failed to connect LDAP: %s" %
                       ldapConnection.result['description'])

#
# Search LDAP using the email addresses associated with every Globus identity.
# If one of them is a match, get the POSIX login username.
#
matches = []
for identity in jsonObj.get('identities', None):
    ldapConnection.search('dc=people,dc=ls-cat,dc=org',
                          ('(mail=%s)' % identity['email']),
                          attributes=['uid'])
    searchResult = ldapConnection.response
    if searchResult != None and len(ldapConnection.entries) > 0:
        matches.append({"id":identity['id'],
                        "output":ldapConnection.entries[0]['uid'].value})

output={"DATA_TYPE":"identity_mapping_output#1.0.0",
        "result":matches}
print(json.dumps(output))
