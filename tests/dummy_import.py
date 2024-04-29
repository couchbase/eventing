from datetime import timedelta
import sys

# needed for any cluster connection
import couchbase.subdocument as SD
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions,
                               QueryOptions)
try:
     from couchbase.exceptions import DocumentNotFoundException
except:
     from couchbase.exceptions import NotFoundError as DocumentNotFoundException

MutationCasMacro = SD.MutationMacro.cas()
DocumentCasMacro = SD.LookupInMacro.cas()

username = "Administrator"
password = "asdasd"
bucket_name = "src"
auth = PasswordAuthenticator(
    username,
    password,
)
cluster = Cluster('couchbase://127.0.0.1:12000', ClusterOptions(auth))
cluster.wait_until_ready(timedelta(seconds=5))

cb = cluster.bucket(bucket_name)
client = cb.scope("_default").collection("_default")

def to_little_endian(input):
    inp_raw = bytearray.fromhex(input[2:])
    inp_raw.reverse()
    return ('0x' + ''.join(f"{n:02X}" for n in inp_raw)).lower()

def perform_import(k):
    try:
        pre_import_result = client.lookup_in(k, [SD.get(DocumentCasMacro, xattr=True)])
    except DocumentNotFoundException:
        client.upsert(k, {})
        pre_import_result = client.lookup_in(k, [SD.get(DocumentCasMacro, xattr=True)])
    except Exception as e:
        print("exception while retreiving current version of document", e)
        return

    # Pre import $document.CAS
    pre_import_cas = pre_import_result.value[0]['value']
    pre_import_cas_processed = to_little_endian(pre_import_cas)
    # print("Prior to import, $document.CAS untreated: ", pre_import_cas)
    print("Prior to import, $document.CAS treated: ", pre_import_cas_processed)

    # Run import
    client.mutate_in(k, [SD.upsert('_mou.importcas', MutationCasMacro, create_parents=True, xattr=True),
                        SD.upsert('_mou.pcas', pre_import_cas_processed, xattr=True)])


    # Post import $document.CAS
    post_import_result = client.lookup_in(k, [SD.get(DocumentCasMacro, xattr=True)])
    post_import_cas = post_import_result.value[0]['value']
    # print("Post import, $document.CAS untreated: ", post_import_cas)
    print("Post import, $document.CAS treated: ", to_little_endian(post_import_cas))

if __name__ == '__main__':
    perform_import(sys.argv[1])
