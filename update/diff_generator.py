import json_delta as jd
from pathlib import Path

def generate_diff(diff_name,
                  prev_path="./update/data/previous",
                  curr_path="./update/data/current_data",
                  output_dir="./update/data/diffs/"):
    """
    Create difference file in output_dir/diff_name
    Return path to difference file.
    """
    try:
        diff_name = Path(diff_name)
        prev_path = Path(prev_path).absolute()
        curr_path = Path(curr_path).absolute()
        output_dir = Path(output_dir).absolute()
        output_path = (output_dir / diff_name).absolute()

        if not prev_path.resolve().exists:
            err_msg = "Path provided for previous data file does not exist."
            err_msg += f"\nprev_path = {prev_path}"
            err_msg += f"\nprev_path.resolve() = {prev_path.resolve()}"
            raise FileNotFoundError

        if not curr_path.resolve().exists:
            err_msg = "Path provided for current data file does not exist."
            err_msg += f"\ncurr_path = {curr_path}"
            err_msg += f"\ncurr_path.resolve() = {curr_path.resolve()}"
            raise FileNotFoundError

        prev_path = prev_path.resolve()
        curr_path = curr_path.resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        with open(prev_path, "rt", encoding="utf-8") as prev:
            previous = jd._util.json.load(prev)

        with open(curr_path, "rt", encoding="utf-8") as curr:
            current = jd._util.json.load(curr)

        diffs = jd.diff(left_struc=previous, right_struc=current)
        assert jd._util.check_diff_structure(diffs)

        with open(output_path.resolve(), "wt", encoding="utf-8") as f:
            try:
                jd._util.json.dump(diffs, f, ensure_ascii=False, sort_keys=True, indent=4)
                print(f"Wrote differences to {output_path.resolve()}.")
                return output_path.resolve()
            except Exception as e:
                raise e

    except FileNotFoundError as e:
        print(err_msg)
        print(e)

    except jd._util.json.decoder.JSONDecodeError as e:
        # TODO: Handle empty files differently than this
        print("Unable to generate diff")
        return 


if __name__ == '__main__':
    generate_diff("test_diff.json", output_dir="./update/data/diffs/")
