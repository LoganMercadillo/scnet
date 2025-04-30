"""
Run this to update the cable database with most recent information.

# TODO IMPLEMENT THE BELOW
If database name specified with optional argument -d, updates that database only.
If no database name specified, updates {main database here} by default. 

If list of websites specified using optional argument -w, calls update-with.py
    to scrape only the specified sites for new data.
If list of websites specified using optional argument --plus, calls update-with.py
    to scrape both the default websites in cable-sites.txt (if it exists; fails if
    this file doesn't exist) AND the list of websites specified.
If no websites specified with -w or --plus, calls update-with.py to scrape
    sites listed in cable-sites.txt for new data.
"""
# import os, tempfile
import time
from pathlib import Path
from shutil import copy2
from json import dump, dumps
from clean_data import parse_data
from diff_generator import generate_diff
from scrapers.scm_scraper import scm_scraper
from write_db import write_db


# def symlink(target, link_name, overwrite=False):
#     '''
#     Create a symbolic link named link_name pointing to target.
#     If link_name exists then FileExistsError is raised, unless overwrite=True.
#     When trying to overwrite a directory, IsADirectoryError is raised.

#     NOTE: I think the path to target must be specified relative to the
#     directory holding link_name, but double check for me and fix this message! 

#     CREDIT TO: This answer https://stackoverflow.com/a/55742015
#     by Tom Hale https://stackoverflow.com/users/5353461/tom-hale
#     '''

#     if not overwrite:
#         os.symlink(target, link_name)
#         return

#     # os.replace() may fail if files are on different filesystems
#     link_dir = os.path.dirname(link_name)

#     # Create link to target with temporary filename
#     while True:
#         temp_link_name = tempfile.mktemp(dir=link_dir)

#         # os.* functions mimic as closely as possible system functions
#         # The POSIX symlink() returns EEXIST if link_name already exists
#         # https://pubs.opengroup.org/onlinepubs/9699919799/functions/symlink.html
#         try:
#             os.symlink(target, temp_link_name)
#             break
#         except FileExistsError:
#             pass

#     # Replace link_name with temp_link_name
#     try:
#         # Pre-empt os.replace on a directory with a nicer message
#         if not os.path.islink(link_name) and os.path.isdir(link_name):
#             raise IsADirectoryError(f"Cannot symlink over existing directory: '{link_name}'")
#         os.replace(temp_link_name, link_name)
#     except:
#         if os.path.islink(temp_link_name):
#             os.remove(temp_link_name)
#         raise


def update_db(
    # TODO: Add paths to distinguish between scm_data and tel_eg_data
    old_data_dir="./update/data/old_data/",
    old_db_dir="./update/db/old_db/",
    new_data_dir="./update/data/",
    new_db_dir="./update/db/",
    prev_symlink_dir="./update/data/",
    initial_run=False
    ):
    #####################################
    #### SETUP DIRECTORIES AND FILES ####
    #####################################
    old_data_dir = Path(old_data_dir).absolute()
    old_db_dir = Path(old_db_dir).absolute()
    new_data_dir = Path(new_data_dir).absolute()
    new_db_dir = Path(new_db_dir).absolute()
    prev_symlink_dir = Path(prev_symlink_dir).absolute()
    # Make the necessary directories, even if this is the initial run of update_db()
    # (we need them and it won't overwrite them if they already exist).
    old_data_dir.mkdir(parents=True, exist_ok=True)
    old_db_dir.mkdir(parents=True, exist_ok=True)
    new_data_dir.mkdir(parents=True, exist_ok=True)
    new_db_dir.mkdir(parents=True, exist_ok=True)
    prev_symlink_dir.mkdir(parents=True, exist_ok=True)

    # Symlinks to the data files for the current and previous scraper data files
    current_data_symlink = (new_data_dir / "current_data").absolute()
    previous_data_symlink = (prev_symlink_dir / "previous_data").absolute()

    if (
        current_data_symlink.is_symlink() and
        current_data_symlink.resolve().exists() and
        previous_data_symlink.is_symlink() and
        previous_data_symlink.resolve().exists()
    ):
        initial_run = False
        print("update run")
    else:
        initial_run = True
        print("initial run")

        # If this is the first run, then there is no file with previous data,
        # so we need to set that up.

        # no_data.json is a placeholder.
        # current_data_symlink and previous symlinks will both with point to it,
        # because they have nothing to point to (no previous and no current yet).
        # After we collect the first data, we will update current_data_symlink
        # to point to it.
        data_placeholder = (new_data_dir / "no_data.json")
        data_placeholder.touch(exist_ok=True)

        current_data_symlink.absolute().unlink(missing_ok=True)
        previous_data_symlink.absolute().unlink(missing_ok=True)

        current_data_symlink.symlink_to(data_placeholder.absolute())
        previous_data_symlink.symlink_to(data_placeholder.absolute())
        print("made symlinks")

    #########################
    #### SCRAPE NEW DATA ####
    #########################
    ## SUBMARINECABLEMAP.COM
    # scm_data = data (dict), scraper_date_uuid = run_date + uuid (str)
    # scraper_date_uuid like "2025-04-28T16:16:07.382_1bf7efba.json"
    scm_data, scraper_date_uuid = scm_scraper(write_log=True)
    scm_file_name = "scm_data_" + scraper_date_uuid + ".json"
    new_scm_data_path = (new_data_dir / scm_file_name).absolute()

    # Write scraped SCM cable data to json file
    with open(new_scm_data_path.resolve(), "wt", encoding="utf-8") as f:
        try:
            dump(scm_data, f, ensure_ascii=False, sort_keys=True, indent=4)
            print(f"Old data: {current_data_symlink.resolve()}")
            print(f"New data: {new_scm_data_path.resolve()}")
            print(f"Log: {scraper_date_uuid}\n")
        except (TypeError, Exception) as e:
            print(e)
            print("Could not write scm cable data.")
            print(f"Log: {scraper_date_uuid}\n")
            print(f"Cables:\n\n {dumps(scm_data.keys(),ensure_ascii=False, sort_keys=True)}")
            exit(3)

    #############################
    #### UPDATE CURR SYMLINK ####
    #############################
    # As of right now,
    # current_data_symlink = (new_data_dir / "current_data").absolute()
    # current_data_symlink.symlink_to(data_placeholder.absolute())

    # save 
    old_curr_data_path = current_data_symlink.resolve().absolute()

    # Overwrite current_data symlink to new data file
    current_data_symlink.unlink(missing_ok=True)
    current_data_symlink.symlink_to(new_scm_data_path.absolute())

    # Copy the now old data file to old_data_dir (moving the file)
    try:
        copy2(old_curr_data_path, old_data_dir)
    except Exception as e:
        pass

    # Now,
    # current_data_symlink = (new_data_dir / "current_data").absolute()
    # current_data_symlink.symlink_to(new_scm_data_path.absolute())
    # so, 
    # current_data_symlink.resolve() = new_scm_data_path.absolute()

    #############################
    #### UPDATE PREV SYMLINK ####
    #############################
    # As of right now,
    # previous_data_symlink = (prev_symlink_dir / "previous_data").absolute()
    # previous_data_symlink.symlink_to(data_placeholder.absolute())
    previous_data_file_name = old_curr_data_path.name

    # Delete the "old" data file from the new_data_dir directory
    old_curr_data_path.unlink(missing_ok=True)

    # Unlink previous_data_symlink from the 
    # previous previous_data_symlink.resolve() in old_data_dir
    # (Deletes the symlink because unlinking symlinks deletes them)
    previous_data_symlink.unlink(missing_ok=True)

    # Link symlink to the moved file in old_data_dir
    previous_data_symlink.symlink_to((old_data_dir / previous_data_file_name).absolute())

    print(f"Previous data now at: {previous_data_symlink.absolute().resolve()}")

    # Now, 
    # previous_data_symlink = (prev_symlink_dir / "previous_data").absolute() # same as before
    # previous_data_symlink.symlink_to((old_data_dir / previous_data_file_name).absolute()) # new

    #########################
    #### UPDATE DATABASE ####
    #########################
    # Get the previous_data_symlink.absolute().resolve()'s date_uuid
    prev_date_uuid = "_".join(
        previous_data_symlink.absolute().resolve().stem.split("_")[-2:]
        )

    # Copy the old database to old_db_dir/scn_prev_date_uuid.db
    new_db_dir = (new_db_dir / "scn.db").absolute()
    old_db_path = (old_db_dir / f"scn_{prev_date_uuid}.db").absolute()
    copy2(
        new_db_dir,
        old_db_path
        )
    print(f"Old database: {old_db_path}")

    # Write cleaned, updated data to new_db_dir/scn.db database
    write_db(
        cleaned_data = parse_data(scm_data),
        data_file=current_data_symlink,
        db_dir=new_db_dir
        )
    if initial_run:
        print(f"New database: {new_db_dir}")
    else:
        print(f"Updated {new_db_dir}")

    #######################
    #### GENERATE DIFF ####
    #######################
    # Generate a difference between the newly scraped data and the previous data
    # Difference file stored in default output_dir="update/data/diffs/"
    # Difference file name ends in scraper_date_uuid
    diff_name = "diffs_after_" +  "_".join(current_data_symlink.resolve().name.split("_")[-2:])
    generate_diff(
        diff_name=diff_name,
        prev_path=previous_data_symlink,
        curr_path=current_data_symlink,
    )

    return


if __name__ == '__main__':
    start_update = time.perf_counter()
    update_db()
    update_done = format((time.perf_counter() - start_update), ".3f")
    print(update_done)
