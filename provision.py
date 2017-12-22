import subprocess as proc

def create_sampling_machine(machine_id, machine_type, startup_script_file):
    MACHINE_BASE_NAME = 'stan-'
    ZONE = 'us-west1-c'
    command_string = ('gcloud compute instances create {name} '
                      '--machine-type {type}'
                      '--zone {zone} '
                      '--image-family {im-fam} '
                      '--image-project {im-proj} '

