# Storetheindex K8S manifests

Storetheindex deployment K8S manifests is managed using GitOps. This means all changes to the
runtime environment are reflected on automatically when the content of this directory changes.

## Secret management

The runtime secrets are managed by the CD pipeline, using [SOPS](https://github.com/mozilla/sops) as
the encryption/decryption that under the hood uses designated KMS keys.

The key designated to storetheindex is specified in a SOPS configuration file in the folder that
contains its manifests for each of the runtime environments. For example, the SOPS config file for
the `prod` environment can be found [here](prod/us-east-2/tenant/storetheindex/.sops.yaml). The
presence of that file instructs all `sops` commands _executed in that directory_ to use the
configured key.

Upon deployment, the CD mechanism decrypts the encrypted secrets on the fly and applies the
unencrypted value to the K8S cluster.

### How to interact with SOPS encrypted secrets

Prerequisites:

* Terminal session authenticated to the AWS account containing the KMS key and the user has the
  privilege to interact with the key.
    * You can check the status of your current AWS session via aws CLI
      command `aws sts get-caller-identity`
* `sops` CLI installed.

To encrypt a file containing sensitive information, e.g. `my-secret.txt`:

* `cd` into the `storetheindex` directory corresponding to the environment you want to make that
  secret available in.
    * For example, `prod/us-east-2/tenant/storetheindex` for the `prod` environment
* Execute `sops -e my-secret.txt > my-secret.encrypted`
    * This command would encrypt the content of `my-secret.txt` and stores the encrypted value to a
      file named `my-secret.encrypted`.
    * Once encrypted, you can now safely delete the unencrypted version from disk and check the
      encrypted file into version control system.
    * Alternatively, you can execute the `sops` command using `-i` flag which performs an in-place
      encryption and replaces the content of the given unencrypted file with its encrypted
      equivalent.

You can now include the encrypted value in your `kustomization.yaml` using `secretGenerator` to
create K8S `Secret` object. An example of this technique can be found in `storetheindex`
Kustomization file in `prod` environments.

It is also possible to define K8S `Secret` object directly, and encrypt its content (i.e. at
key `data` or `stringData`) using SOPS.

To examine/edit the value of an encrypted file checked into version control system:

* `cd` into the directory that contains the encrypted file.
* Execute `sops <encrypted-file>`
    * This command would open the unencrypted content of the file in your default `EDITOR`.
* Edit the content as you see fit and once finished, save and close you editor.
* Upon closing the editor, `sops` will automatically re-encrypt the content to reflect your changes.

Alternatively, use `-d` flag to output the content of a decrypted file. For more information,
execute `sops -h`. 
