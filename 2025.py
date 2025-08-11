import ast
import os
import time
import sys
import pandas as pd
import math
from datetime import datetime
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from zipfile import BadZipFile
from pysat.solvers import Glucose3
from pysat.formula import CNF

M = 2  # Số máy (có thể điều chỉnh)
# ---------- Log ----------
log_file = open('run.log', 'a')
def log(*args, **kwargs):
    print(*args, **kwargs)
    print(*args, file=log_file, **kwargs)
    log_file.flush()

# ---------- Ghi Excel ----------
def write_to_excel(result_dict):
    df = pd.DataFrame([result_dict])
    date = datetime.now().strftime('%Y-%m-%d')
    output_dir = 'out'
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    file_path = f'{output_dir}/results_{date}.xlsx'

    if os.path.exists(file_path):
        try:
            book = load_workbook(file_path)
        except BadZipFile:
            book = Workbook()
        if 'Results' not in book.sheetnames:
            book.create_sheet('Results')
        sheet = book['Results']
        for row in dataframe_to_rows(df, index=False, header=False):
            sheet.append(row)
        book.save(file_path)
    else:
        df.to_excel(file_path, index=False, sheet_name='Results', header=True)

    log(f"Ghi kết quả vào Excel: {os.path.abspath(file_path)}")

# ---------- Hàm biến ----------
def var_x(i, t, m, M, T):
    return 1 + i * M * T + t * M + m

def y_offset(N, M, T):
    return 1 + N * M * T

def var_y(m, t1, t2, M, T, N):
    return y_offset(N, M, T) + m * T * T + t1 * T + t2

# ---------- Mã hóa bài toán ----------
def encode_schedule_problem(N, M, T, d, r, e):
    """
    Encode scheduling with:
    - x_{i,t,m} : task i starts at time t on machine m
    - y_{m,t1,t2}: machine m is occupied on interval [t1,t2]
    We create auxiliary block-vars y_block_{m,l,r} representing "some interval
    with endpoints in block l and r is chosen on machine m".
    This reduces pairwise non-overlap clauses by grouping intervals into blocks.
    """
    cnf = CNF()
    active_y = set()

    # Ensure T positive
    if T <= 0:
        return cnf

    # ---------- R1: x -> y (if x(i,t,m) then the corresponding y interval is true)
    # Note: restrict t so that t + d[i] <= T (valid end)
    for i in range(N):
        for m in range(M):
            # t must be such that interval [t, t+d[i]] is inside [0, T]
            t_start = max(0, r[i])
            t_end_allowed = min(e[i] - d[i], T - d[i])  # inclusive
            if t_start > t_end_allowed:
                continue
            for t in range(t_start, t_end_allowed + 1):
                t_end = t + d[i]
                xi = var_x(i, t, m, M, T)
                yi = var_y(m, t, t_end, M, T, N)
                # x -> y
                cnf.append([-xi, yi])
                # record active y (mach, t1, t2, varid)
                active_y.add((m, t, t_end, yi))

    # ---------- R2: each task must be scheduled at least once (big OR)
    for i in range(N):
        lits = []
        t_start = max(0, r[i])
        t_end_allowed = min(e[i] - d[i], T - d[i])
        if t_start <= t_end_allowed:
            for m in range(M):
                for t in range(t_start, t_end_allowed + 1):
                    lits.append(var_x(i, t, m, M, T))
        # if lits empty then no possible start -> CNF will be UNSAT automatically
        if lits:
            cnf.append(lits)
        else:
            # add an empty clause to make UNSAT immediately
            cnf.append([])

    # ---------- R3: Non-overlap for y on same machine using block decomposition
    # Choose block size b ~ sqrt(T) (heuristic)
    b = max(1, int(math.sqrt(max(1, T))))
    k = (T + b - 1) // b  # number of blocks

    def block_of(t):
        return t // b

    # Build list of intervals per machine and per block-pair
    # active_y elements: (m, t1, t2, yi)
    intervals_by_machine = {m: [] for m in range(M)}
    for (mach, t1, t2, yi) in active_y:
        # only consider valid intervals (t1 < t2 and in range)
        if 0 <= t1 < t2 <= T:
            intervals_by_machine[mach].append((t1, t2, yi))

    # determine current max var id to allocate fresh auxiliary vars
    max_existing = 0
    # find max from active_y and from x variables (we can estimate upper bound)
    # upper bound for x variables:
    max_existing = max(max_existing, y_offset(N, M, T) + M * T * T + 10)
    # also check actual yi values
    for (_, _, _, yi) in active_y:
        if yi > max_existing:
            max_existing = yi
    next_aux = max_existing + 1

    # store block-var ids
    yblock_id = {}  # (m, l, r) -> varid
    block_to_intervals = {}  # (m,l,r) -> list of yi

    for m in range(M):
        # collect intervals into block-pairs
        for (t1, t2, yi) in intervals_by_machine[m]:
            l = block_of(t1)
            rblk = block_of(t2 - 1)  # endpoint t2 is exclusive-ish; ensure proper block
            # clamp to [0, k-1]
            l = min(max(0, l), k - 1)
            rblk = min(max(0, rblk), k - 1)
            key = (m, l, rblk)
            if key not in block_to_intervals:
                block_to_intervals[key] = []
            block_to_intervals[key].append((t1, t2, yi))

    # create block-vars and add linking clauses
    for key, intervals in block_to_intervals.items():
        m, l, rblk = key
        yb = next_aux
        next_aux += 1
        yblock_id[key] = yb
        # For each interval yi in this block-pair: yi -> yb
        for (_, _, yi) in intervals:
            cnf.append([-yi, yb])
        # yb -> OR intervals  i.e. (¬yb ∨ yi1 ∨ yi2 ∨ ...)
        clause = [-yb] + [yi for (_, _, yi) in intervals]
        cnf.append(clause)

    # Now add non-overlap at block level: yblock vars that can represent overlapping intervals must not both be true
    # Two block-pairs (l1,r1) and (l2,r2) MAY represent overlapping intervals unless r1 < l2 or r2 < l1
    # iterate per machine
    for m in range(M):
        # get list of block-pairs for this machine
        block_pairs = [(l, r) for (mm, l, r) in yblock_id.keys() if mm == m]
        # compare pairs
        for i in range(len(block_pairs)):
            l1, r1 = block_pairs[i]
            yb1 = yblock_id[(m, l1, r1)]
            for j in range(i + 1, len(block_pairs)):
                l2, r2 = block_pairs[j]
                yb2 = yblock_id[(m, l2, r2)]
                # if block-pairs can correspond to overlapping intervals:
                if not (r1 < l2 or r2 < l1):
                    cnf.append([-yb1, -yb2])

    # Finally, for intervals that lie wholly inside the same block (l == r), we must ensure they don't overlap inside block.
    # We'll add pairwise non-overlap for intervals that share the same block (this is limited since block size is small).
    for key, intervals in block_to_intervals.items():
        m, l, rblk = key
        if l == rblk:
            # intervals list holds (t1,t2,yi)
            for i in range(len(intervals)):
                t1, t2, yi = intervals[i]
                for j in range(i + 1, len(intervals)):
                    s1, s2, yj = intervals[j]
                    # check if intervals overlap
                    if not (t2 <= s1 or s2 <= t1):
                        cnf.append([-yi, -yj])

    return cnf

# ---------- Giải và ghi kết quả ----------
def solve_and_record(task_id, problem_name, N, M, T, d, r, e):
    log(f"\n🔍 Bắt đầu giải bài toán: {problem_name} (ID: {task_id})")
    log(f"Task({r}, {e}, {d}), Machines: {M}, Time slots: {T}")
    start = time.time()
    cnf = encode_schedule_problem(N, M, T, d, r, e)
    solver = Glucose3()
    for clause in cnf.clauses:
        solver.add_clause(clause)

    status = solver.solve()
    elapsed = time.time() - start

    if status:
        model = solver.get_model()
        log("→ Lịch tìm được:")
        for i in range(N):
            for t in range(T):
                for m in range(M):
                    if var_x(i, t, m, M, T) in model:
                        log(f"Tác vụ {i} gán cho máy {m} từ thời điểm {t} đến {t + d[i]}")
                        break
                else:
                    continue
                break
    else:
        log("⛔ Không tìm được lịch hợp lệ.")

    result = {
        "ID": task_id,
        "Problem": problem_name,
        "Type": "biclique",
        "Time": round(elapsed, 4),
        "Result": "SAT" if status else "UNSAT",
        "Variables": solver.nof_vars(),
        "Clauses": solver.nof_clauses()
    }
    solver.delete()
    write_to_excel(result)

# ---------- Xử lý thư mục đầu vào ----------
def process_input_dir(input_dir):
    task_id = 1
    for filename in sorted(os.listdir(input_dir)):
        if filename.endswith(".txt"):
            path = os.path.join(input_dir, filename)
            with open(path) as f:
                N = int(f.readline().strip())
                tasks = ast.literal_eval(f.readline().strip())
                r = [task[0] for task in tasks]
                d = [task[1] for task in tasks]
                e = [task[2] for task in tasks]
                T = max(e)  # Thời gian tối đa cần xét
                log(f"\n🎯 Đang xử lý: {filename}")
                solve_and_record(task_id, filename, N, M, T, d, r, e)
                task_id += 1
    log_file.close()

# ---------- Gọi từ terminal ----------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❗ Sử dụng: python scheduler_sat.py <tên_thư_mục_input>")
    else:
        input_dir = os.path.join("input", sys.argv[1])
        process_input_dir(input_dir)