import argparse
import io
import os
import sys
import time
import traceback

from typing import Dict, Optional, Sequence

from qumulo.rest_client import RestClient

from . import FileInfo, Worker

DEBUG = False
if os.getenv("QDEBUG"):
    DEBUG = True


def log_it(msg: str) -> None:
    if DEBUG:
        print("%s: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg))
        sys.stdout.flush()


class CopyDirDates:
    def __init__(self, in_args: Sequence[str]):
        parser = argparse.ArgumentParser(description="")
        parser.add_argument("--to_dir", help="destination directory")
        parser.add_argument(
            "--skip_hardlinks", help=" skip hard links", action="store_true"
        )
        parser.add_argument(
            "--no_preserve",
            help="will not preserve permissions or timestamps",
            action="store_true",
        )
        args = parser.parse_args(in_args)
        self.to_dir: Optional[str] = None
        self.skip_hardlinks: Optional[bool] = None
        self.no_preserve: Optional[bool] = None
        self.cols = ["path"]
        if args.to_dir:
            self.to_dir = args.to_dir
        if args.skip_hardlinks:
            self.skip_hardlinks = args.skip_hardlinks
        if args.no_preserve:
            self.no_preserve = args.no_preserve
        self.folders: Dict[str, str] = {}


    def every_batch(self, file_list: Sequence[FileInfo], work_obj: Worker) -> None:
        results = []
        for file_obj in file_list:
            try:
                to_path = file_obj["path"]
                if self.to_dir is not None:
                    to_path = to_path.replace(work_obj.start_path, self.to_dir)

                parent_path = os.path.dirname(to_path)
                file_name = os.path.basename(to_path)
                if file_obj["type"] == "FS_FILE_TYPE_DIRECTORY":

                    new_f = work_obj.rc.fs.get_file_attr(path=to_path)
                    file_exists = new_f['id']

                    o_attr = work_obj.rc.fs.get_file_attr(
                        snapshot = work_obj.snap,
                        id_ = file_obj["id"]
                    )

                    if not self.no_preserve:
                        work_obj.rc.fs.set_file_attr(
                            id_ = file_exists,
                            owner=o_attr['owner'],
                            group=o_attr['group'],
                            extended_attributes=o_attr['extended_attributes'],
                        )

                        work_obj.rc.fs.set_file_attr(
                            id_ = file_exists,
                            creation_time = o_attr['creation_time'],
                            modification_time = o_attr['modification_time'],
                            change_time = o_attr['change_time'],
                        )

                    results.append("DIRECTORY : %s -> %s" % (file_obj["path"], to_path))

            except:
                log_it("Other exception: %s %s" % (sys.exc_info(), file_obj["path"]))
                results.append(
                    "!!FILE COPY FAILED3: %s -> %s" % (sys.exc_info(), file_obj["path"])
                )

        try:
            if len(results) > 0:
                with work_obj.result_file_lock:
                    with io.open(work_obj.LOG_FILE_NAME, "a", encoding="utf8") as f:
                        for d in results:
                            f.write("%s\n" % d)
                    work_obj.action_count.value += len(results)
        except:
            log_it("Unable to save results exception: %s" % str(sys.exc_info()))

    @staticmethod
    def work_start(work_obj: Worker) -> None:
        if os.path.exists(work_obj.LOG_FILE_NAME):
            os.remove(work_obj.LOG_FILE_NAME)

    @staticmethod
    def work_done(_work_obj: Worker) -> None:
        pass
