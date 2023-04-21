import json
import os.path
from pathlib import Path

import charliecloud as ch
import build_cache as bu
import image as im
import registry as rg
import version


## Main ##

def main(cli):
   src_ref = im.Reference(cli.source_ref)
   ch.INFO("pushing image:   %s" % src_ref)
   image = im.Image(src_ref, cli.image)
   # FIXME: validate it's an image using Megan's new function (PR #908)
   if (not os.path.isdir(image.unpack_path)):
      if (cli.image is not None):
         ch.FATAL("can't push: %s does not appear to be an image" % cli.image)
      else:
         ch.FATAL("can't push: no image %s" % src_ref)
   if (cli.image is not None):
      ch.INFO("image path:      %s" % image.unpack_path)
   else:
      ch.VERBOSE("image path:      %s" % image.unpack_path)
   if (cli.dest_ref is not None):
      dst_ref = im.Reference(cli.dest_ref)
      ch.INFO("destination:     %s" % dst_ref)
   else:
      dst_ref = im.Reference(cli.source_ref)
   up = Image_Pusher(image, dst_ref, cli.cache_upload)
   up.push()
   ch.done_notify()


## Classes ##

class Image_Pusher:

   # Note; We use functions to create the blank config and manifest to
   # avoid copy/deepcopy complexity from just copying a default dict.

   __slots__ = ("cache_upload",  # bool if upload cache is enabled
                "config",        # sequence of bytes
                "dst_ref",       # destination of upload
                "image",         # Image object we are uploading
                "layers",        # list of (digest, .tar.gz path), lowest first
                "manifest")      # sequence of bytes

   def __init__(self, image, dst_ref, cache_upload):
      self.cache_upload = cache_upload
      self.config = None
      self.dst_ref = dst_ref
      self.image = image
      self.layers = None
      self.manifest = None

   @classmethod
   def config_new(class_):
      "Return an empty config, ready to be filled in."
      # FIXME: URL of relevant docs?
      # FIXME: tidy blank/empty fields?
      return { "architecture": ch.arch_host_get(),
               "charliecloud_version": version.VERSION,
               "comment": "pushed with Charliecloud",
               "config": {},
               "container_config": {},
               "created": ch.now_utc_iso8601(),
               "history": [],
               "os": "linux",
               "rootfs": { "diff_ids": [], "type": "layers" },
               "weirdal": "yankovic" }

   @classmethod
   def manifest_new(class_):
      "Return an empty manifest, ready to be filled in."
      return { "schemaVersion": 2,
               "mediaType": rg.TYPES_MANIFEST["docker2"],
               "config": { "mediaType": rg.TYPE_CONFIG,
                           "size": None,
                           "digest": None },
               "layers": [],
               "weirdal": "yankovic" }

   def cleanup(self):
      ch.INFO("cleaning up")
      # Delete the config, manifest, and tarballs if the cache is disabled.
      conf_suffix = str(self.image) + ".config.json"
      conf_path = ch.storage.upload_cache // conf_suffix
      ch.VERBOSE("deleting config: %s" % conf_path)
      conf_path.unlink()
      man_suffix = str(self.image) + ".manifest.json"
      man_path = ch.storage.upload_cache // man_suffix
      ch.VERBOSE("deleting manifest: %s" % man_path)
      man_path.unlink()
      for (_, tar_c) in self.layers:
         ch.VERBOSE("deleting tarball: %s" % tar_c)
         tar_c.unlink_()

   def prepare_upload(self, config, manifest, tars_c):
      """Store a list of gzipped layer tarball paths, config as a sequence of
       bytes, and manifest as a sequence of bytes."""
      config_bytes = json.dumps(config, indent=2).encode("UTF-8")
      config_hash = ch.bytes_hash(config_bytes)
      manifest["config"]["size"] = len(config_bytes)
      manifest["config"]["digest"] = "sha256:" + config_hash
      ch.DEBUG("config: %s\n%s" % (config_hash, config_bytes.decode("UTF-8")))
      manifest_bytes = json.dumps(manifest, indent=2).encode("UTF-8")
      ch.DEBUG("manifest:\n%s" % manifest_bytes.decode("UTF-8"))
      # Store for the next steps.
      self.layers = tars_c
      self.config = config_bytes
      self.manifest = manifest_bytes

   def prepare_existing(self):
      """Attempt to reuse previously prepared self.image config, manifest,
         and layer files."""
      ch.DEBUG("checking for previously prepared files")
      (sid, git_hash) = bu.cache.find_image(self.image)
      if ((sid, git_hash) is (None, None)):
         return
      conf_suffix = git_hash + ".config.json"
      conf_path = ch.storage.upload_cache // conf_suffix
      if (not conf_path.exists()):
         return
      config = conf_path.json_from_file("config")
      man_suffix = git_hash + ".manifest.json"
      man_path = ch.storage.upload_cache // man_suffix
      if (not man_path.exists()):
         return
      manifest = man_path.json_from_file("manifest")
      tar_c = git_hash + ".tar.gz"
      path_c = ch.storage.upload_cache // tar_c
      if (not path_c.exists()):
         return
      hash_c = path_c.file_hash()
      tars_c = [(hash_c, path_c)]
      self.prepare_upload(config, manifest, tars_c)
      # Don't cleanup if existing files were used
      self.cache_upload = True

   def prepare_new(self):
      """Prepare new config, manifest, and layer files for pushing."""
      tars_uc = self.image.tarballs_write(ch.storage.upload_cache)
      tars_c = list()
      config = self.config_new()
      manifest = self.manifest_new()
      (sid, git_hash) = bu.cache.find_image(self.image)
      # Prepare layers.
      for (i, tar_uc) in enumerate(tars_uc, start=1):
         ch.INFO("layer %d/%d: preparing" % (i, len(tars_uc)))
         path_uc = ch.storage.upload_cache // tar_uc
         hash_uc = path_uc.file_hash()
         config["rootfs"]["diff_ids"].append("sha256:" + hash_uc)
         size_uc = path_uc.file_size()
         path_c = path_uc.file_gzip(["-9", "--no-name"])
         if (not isinstance(bu.cache, bu.Disabled_Cache)
             and self.cache_upload):
            tar_suffix = git_hash + ".tar.gz"
            path_c.rename_(ch.storage.upload_cache // tar_suffix)
            path_c = ch.storage.upload_cache // tar_suffix
         hash_c = path_c.file_hash()
         size_c = path_c.file_size()
         manifest["layers"].append({ "mediaType": rg.TYPE_LAYER,
                                     "size": size_c,
                                     "digest": "sha256:" + hash_c })
         tars_c.append((hash_c, path_c))
      # Prepare metadata.
      ch.INFO("preparing metadata")
      self.image.metadata_load()
      # Environment. Note that this is *not* a dictionary for some reason but
      # a list of name/value pairs separated by equals [1], with no quoting.
      #
      # [1]: https://github.com/opencontainers/image-spec/blob/main/config.md
      config['config']['Env'] = ["%s=%s" % (k, v)
                                 for k, v
                                 in self.image.metadata.get("env", {}).items()]
      # History. Some registries, e.g., Quay, use history metadata for simple
      # sanity checks. For example, when an image's number of "empty_layer"
      # history entries doesn't match the number of layers being uploaded,
      # Quay will reject the image upload.
      #
      # This type of error checking is odd as the empty_layer key is optional
      # (https://github.com/opencontainers/image-spec/blob/main/config.md).
      #
      # Thus, to push images built (or pulled) with Charliecloud we ensure the
      # the total number of non-empty layers always totals one (1). To do this
      # we iterate over the history entires backward searching for the first
      # non-empty entry and preserve it; all others are set to empty.
      hist = self.image.metadata["history"]
      non_empty_winner = None
      for i in range(len(hist) - 1, -1, -1):
         if (   "empty_layer" not in hist[i].keys()
             or (    "empty_layer" in hist[i].keys()
                 and not hist[i]["empty_layer"] == True)):
            non_empty_winner = i
            break
      assert(non_empty_winner is not None)
      for i in range(len(hist) - 1):
         if (i != non_empty_winner):
            hist[i]["empty_layer"] = True
      config["history"] = hist
      if (not isinstance(bu.cache, bu.Disabled_Cache)
          and self.cache_upload):
         conf_suffix = git_hash + ".config.json"
         man_suffix = git_hash + ".manifest.json"
      else:
         conf_suffix = str(self.image) + ".config.json"
         man_suffix = str(self.image) + ".manifest.json"
      conf_path = ch.storage.upload_cache // conf_suffix
      man_path = ch.storage.upload_cache // man_suffix
      with open(conf_path, "w") as conf:
         json.dump(config, conf)
      with open(man_path, "w") as man:
         json.dump(manifest, man)
      self.prepare_upload(config, manifest, tars_c)

   def prepare(self):
      """Prepare self.image for pushing to self.dst_ref. First, check for an
         existing config, manifest, and gziped layer tarball of the image. If
         not existing, prepare them. Then store the config as a sequence of
         bytes, manifest as a sequence of bytes, and layers as a list of
         gzipped layer tarball paths."""
      # If build cache is disabled, we can't know if the image has changed
      # since the last push.
      if (not isinstance(bu.cache, bu.Disabled_Cache)):
         self.prepare_existing()
      # If any previously prepared files are missing, replace them all.
      if (   self.config is None or self.manifest is None
          or self.layers is None):
         self.prepare_new()

   def push(self):
      self.prepare()
      self.upload()
      if (isinstance(bu.cache, bu.Disabled_Cache) or not self.cache_upload):
         self.cleanup()

   def upload(self):
      ch.INFO("starting upload")
      ul = rg.HTTP(self.dst_ref)
      for (i, (digest, tarball)) in enumerate(self.layers, start=1):
         ul.layer_from_file(digest, tarball,
                            "layer %d/%d: " % (i, len(self.layers)))
      ul.config_upload(self.config)
      ul.manifest_upload(self.manifest)
      ul.close()
