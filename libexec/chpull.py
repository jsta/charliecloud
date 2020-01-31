#!%PYTHON_SHEBANG%

# Pull and unpack an image from the open docker registry.
#
# This script is experimental; a proof of concept. This script does not store
# image data in OCI format; the image layers are downloaded and unpacked.
#
# Future work may include maniging image changesets, e.g., update and/or
# removals carried out by ch-grow.

import argparse
import collections
import json
import os
import re
import requests
import shutil
import sys
import tarfile

from hashlib import sha256

## Globals ##
session = requests.Session()

## Constants ##

# FIXME: assign with argument; else default
registryBase = 'https://registry-1.docker.io'
authBase     = 'https://auth.docker.io'
authService  = 'registry.docker.io'

# Accepted Docker V2 media types.
# See https://docs.docker.com/registry/spec/manifest-v2-2/
MF_SCHEMA1 = 'application/vnd.docker.distribution.manifest.v1+json'
MF_SCHEMA2 = 'application/vnd.docker.distribution.manifest.v2+json'
MF_LIST    = 'application/vnd.docker.distribution.manifest.list.v2+json'
C_CONFIG   = 'application/vnd.docker.container.image.v1+json'
LAYER      = 'application/vnd.docker.image.rootfs.diff.tar.gzip'
PLUGINS    = 'application/vnd.docker.plugin.v1+json'

PROXIES = { "HTTP_PROXY":  os.environ.get("HTTP_PROXY"),
            "HTTPS_PROXY": os.environ.get("HTTPS_PROXY"),
            "FTP_PROXY":   os.environ.get("FTP_PROXY"),
            "NO_PROXY":    os.environ.get("NO_PROXY"),
            "http_proxy":  os.environ.get("http_proxy"),
            "https_proxy": os.environ.get("https_proxy"),
            "ftp_proxy":   os.environ.get("ftp_proxy"),
            "no_proxy":    os.environ.get("no_proxy"),
}

## Classes ##

class Image:
    def __init__(self, name, reference, tag):
        # ATTRIBUTE      TYPE          DESCRIPTION
        # 1. name        string        Image name.
        # 2. reference   string        Repository image reference (tag|digest)
        # 3. tag         string        Image tag.
        # 4. session     Session       Session with token authorization.
        # 5. manifest    http reponse  Image manifest.
        # 6. image_id    string        Computed sha256 hash of manifest dump.
        # 7. layers      list          Image layer digests.
        self.name        = name
        self.reference   = reference
        self.tag         = tag
        self.session     = MySession(self)
        self.manifest    = self.data_fetch('manifests',
                                           MF_SCHEMA2,
                                           self.reference)
        self.image_id    = self.compute_json_hash()
        self.layers      = self.layer_list_get()

    def compute_json_hash(self):
        return sha256(json.dumps(self.manifest.json(),
                                 indent=3).encode()).hexdigest()

    def data_fetch(self, branch, media, reference):
        self.session.headers.update({ 'Accept': media })
        URL = "{}/v2/{}/{}/{}".format(registryBase,
                                      self.name,
                                      branch,
                                      reference)
        # FIXME: change to DEBUG
        print("GET {}".format(URL))
        try:
            response = session.get(URL,
                                   headers=self.session.headers,
                                   proxies=PROXIES)
            response.raise_for_status()
        except HTTPError as http_err:
            print('HTTP error: {}'.format(http_err))
        except Exception as err:
            print('non HTTP error occured: {}'.format(err))
        else:
            # FIXME: convert to DEBUG
            print('Success')
        return response

    def layer_list_get(self):
        layers = list()
        for layer in self.manifest.json().get('layers'):
            layers.append(layer.get('digest'))
        return layers

    def unpack(self, dst):
        if os.path.isdir(dst):
            shutil.rmtree(dst)
        os.makedirs(dst)
        os.chdir(dst)

        # FIXME: improve algo
        # for each layer digest:
        #   1) pull the layer tar archive
        #   2) parse the tar layer archive
        #         if device file then faile
        #         elif whiteout then add to wh list
        #         else add file to list of members to extract
        #   4) for each whiteout file in list:
        #       a) check for the presence of whiteout filepath in CWD (image dir)
        #       b) if file exists, remove it; otherwise error
        #   4) unpack tar with specified members
        for layer in self.layers:
            whiteouts = list()
            members   = list()
            file_ = layer.split('sha256:')[-1]
            r = self.data_fetch('blobs', 'layer', layer)
            open(file_, 'wb').write(r.content)

            if not tarfile.is_tarfile(file_):
                print('fetched layer {} is not a valid tarfile'.format(file_))
                sys.exit(1)

            tf = tarfile.open(file_, 'r')
            tf_info = tf.getmembers()

            for f in tf_info:
                if re.search('\.wh\.*', f.name):
                    whiteouts.append(f.name)
                elif f.isdev():
                    print('unsupported: tarfile containers device files')
                    sys.exit(1)
                else:
                    members.append(f)

            # look in image for whiteout files
            for wh in whiteouts:
                # TODO: handle opaque whiteouts
                wf = os.path.basename(wh)
                path_to_wf = os.path.dirname(wh)
                file_to_delete = os.path.join(path_to_wf, wf.split('.wh.')[-1])
                if os.path.exists(file_to_delete):
                    if os.path.isdir(file_to_delete):
                        shutil.rmtree(file_to_delete)
                    else:
                        os.remove(file_to_delete)
                else:
                    # FIXME: sane output
                    print("error: whiteoutfile specified but not found:")
                    print("wh = {}".format(wh))
                    print("wf = {}".format(wf))
                    print("file_to_delete = {}".format(file_to_delete))
                    sys.exit(1)
            tf.extractall(members=members) # don't unpack whiteout files
            tf.close()
            os.remove(file_)


class MySession:
    def __init__(self, image):
        self.token   = self.get_token(image)
        self.headers = self.get_headers()

    def get_headers(self):
        return {'Authorization': 'Bearer {}'.format(self.token)}

    def get_token(self, image):
        tokenService = '{}/token?service={}'.format(authBase, authService)
        scopeRepo    = '&scope=repository:{}:pull'.format(image.name)
        authURL      = tokenService + scopeRepo
        return session.get(authURL, proxies=PROXIES).json()['token']


## Supporting functins ##

# FIXME: implement Debug function. Should this stuff just be done in ch-grow?
#        unsure of duplicating code

## Main ##

def main():
    ap = argparse.ArgumentParser(
         formatter_class=argparse.RawDescriptionHelpFormatter,
         description='Pull images from Docker repository and unpack them for "ch-grow" ingestion.',
         epilog="""\
  CH_GROW_STORAGE       default for --storage
""")
    ap.add_argument("image",
                    type=str,
                    metavar="IMAGE[:TAG][@DIGEST]",
                    nargs=1,
                    help="image name")
    ap.add_argument("-s", "--storage",
                    type=str,
                    metavar="DIR",
                    nargs=1,
                    help="image storage directory (default: /var/tmp/ch-grow",
                    default=os.environ.get("CH_GROW_STORAGE",
                                           "/var/tmp/ch-grow"))
    ap.add_argument("-v", "--version",
                    action=CLI_Version,
                    help="print version and exit")

    if (len(sys.argv) < 2):
        ap.print_help(file=sys.stderr)
        sys.exit(1)

    args = ap.parse_args()

    # FIXME: chpull should work on it's own (otherwise it should be in lib, not
    #        libexec).

    # Docker defines a valid image target as IMAGE[:TAG][@DIGEST] where IMAGE
    # is the image name, TAG is the image tag, e.g., 'latest', '3.9', etc., and
    # DIGEST is an algorithm and hash deliminated by a colon, e.g.,
    # 'hello-world:latest@sha256:(some hash here)'.
    #
    # To get a image manifest and it's blobs we need the image name and a
    # reference (either tag or hash). The following splits the user input and
    # determines the name and reference (with digest having priority over tag).
    img = args.image[0].split('@')
    if len(img) == 1:
        name, tag = split_image_tag(args.image[0])
        reference = tag
    elif len(img) == 2:
        name, tag = split_image_tag(img[0])
        reference = img[-1]
    else:
        print("error: image: invalid image syntax '{}'".format(args.image[0]))
        sys.exit(1)

    # TODO: implement something
    # image = Image(name, reference, tag, args.storage)

    return 0

## Bootstrap ##

if __name__ == "__main__":
    main()
