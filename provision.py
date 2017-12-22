import subprocess as proc

def create_sampling_machine(logger, machine_id, machine_type, boot_disk_size, startup_script_file):
    MACHINE_BASE_NAME = 'stan-'
    ZONE = 'us-west1-c'
    SCOPES = 'storage-rw'
    IMAGE_FAMILY = 'ubuntu-1704'
    IMAGE_PROJECT = 'ubuntu-os-cloud'
    command_template = ('gcloud compute instances create {name} '
                      '--machine-type {mach_type}'
                      '--zone {zone} '
                      '--image-family {im_fam} '
                      '--image-project {im_proj} '
                      '--boot-disk-size {size}GB '
                      '--scopes {scopes} '
                      '--metadata-from-file startup-script={startup_file} '
                      '--metadata machine_id={mach_id} '
                     )
    command_string = command_template.format(
        name=(MACHINE_BASE_NAME + str(machine_id)),
        mach_type=machine_type,
        zone=ZONE,
        im_fam=IMAGE_FAMILY,
        im_proj=IMAGE_PROJECT,
        size=boot_disk_size,
        scope=SCOPES
        startup_file=startup_script_file,
        mach_id=machine_id)
    logger.log(
        message='Spinning up machine {} with command {}'.format(
            machine_id, command_string)
        user_id=machine_id
    )
    try:
        proc.run(command_string, shell=True)
    except proc.CalledProcessError as e:
        logger.log('VM creation failed!')
        #logger.exception_log(e)
    return (MACHINE_BASE_NAME + str(machine_id))


