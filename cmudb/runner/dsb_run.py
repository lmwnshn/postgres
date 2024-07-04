import json
import os
import time
import traceback
from pathlib import Path
from typing import Optional

import psycopg.errors
import sqlalchemy.exc
from dsb_rewriter import *
from sqlalchemy import Connection, Engine, NullPool, create_engine
from tqdm import tqdm
from util import (
    conn_execute,
    connstr,
    prewarm_all,
    sql_file_queries,
    vacuum_analyze_all,
)


class Config:
    def __init__(
        self,
        expt_name: str,
        timeout_s: int = 60 * 5,
        rewriter: Optional[Rewriter] = None,
        before_sql: Optional[list[str]] = None,
        after_sql: Optional[list[str]] = None,
    ):
        self.expt_name = expt_name
        self.timeout_s = timeout_s
        self.rewriter = rewriter if rewriter is not None else EARewriter()
        self.before_sql = before_sql if before_sql is not None else []
        self.after_sql = after_sql if after_sql is not None else []


def dsb(engine: Engine, conn: Connection, config: Config, verbose=False):
    artifact_root = Path(os.getenv("ARTIFACT_ROOT"))
    query_root = Path(os.getenv("DSB_QUERY_ROOT"))
    train_seed = int(os.getenv("DSB_QUERY_TRAIN_SEED"))
    test_seed = int(os.getenv("DSB_QUERY_TEST_SEED"))
    dsb_sf = int(os.getenv("DSB_SF"))

    readied = False
    executed_once = False

    seeds = [train_seed]
    if config.expt_name == "default":
        seeds.append(test_seed)

    for seed in tqdm(seeds, desc=f"{config.expt_name} {dsb_sf} DSB seeds.", leave=None):
        outdir = (
            artifact_root
            / "experiment"
            / config.expt_name
            / "dsb"
            / f"sf_{dsb_sf}"
            / "default"
            / str(seed)
        )
        outdir.mkdir(parents=True, exist_ok=True)

        for query_path in tqdm(
            sorted(
                (query_root / "default" / str(seed)).glob("*.sql"),
                key=lambda s: str(s).split("-")[0],
            ),
            desc=f"{config.expt_name} {dsb_sf} DSB query.",
            leave=None,
        ):
            query_id = str(query_path.stem)
            for query_subnum, query in enumerate(
                sql_file_queries(query_path.absolute()), 1
            ):
                outpath_ok = outdir / f"{query_path.stem}-{query_subnum}.ok"
                outpath_res = outdir / f"{query_path.stem}-{query_subnum}.res"
                outpath_err = outdir / f"{query_path.stem}-{query_subnum}.err"
                outpath_timeout = outdir / f"{query_path.stem}-{query_subnum}.timeout"

                if outpath_err.exists():
                    continue

                if outpath_ok.exists():
                    continue

                try:
                    if not readied:
                        conn_execute(
                            conn, f"SET statement_timeout = '0s'", verbose=verbose
                        )
                        prewarm_all(engine, conn, verbose=verbose)
                        vacuum_analyze_all(engine, conn, verbose=verbose)
                        for sql in config.before_sql:
                            conn_execute(conn, sql, verbose=verbose)
                        conn_execute(
                            conn,
                            f"SET statement_timeout = '{config.timeout_s}s'",
                            verbose=verbose,
                        )
                        readied = True
                        executed_once = True

                    with open(outpath_res, "w") as output_file:
                        query, is_ea = config.rewriter.rewrite(
                            query_id, query_subnum, query
                        )

                        result = conn_execute(conn, query, verbose=False)
                        if is_ea:
                            ea_result = str(result.fetchone()[0][0])
                            print(ea_result, file=output_file)
                        outpath_ok.touch(exist_ok=True)

                except sqlalchemy.exc.OperationalError as e:
                    if isinstance(e.orig, psycopg.errors.QueryCanceled):
                        # Since DSB's data distribution varies, timeouts may not be shared.
                        outpath_timeout.touch(exist_ok=True)
                        outpath_ok.touch(exist_ok=True)
                    else:
                        raise e

                except sqlalchemy.exc.DataError as e:
                    if isinstance(e.orig, psycopg.errors.DivisionByZero):
                        with open(outpath_err, "w") as error_file:
                            traceback.print_exception(e, file=error_file)
                        outpath_ok.touch(exist_ok=True)
                    else:
                        raise e

    if executed_once:
        for sql in config.after_sql:
            conn_execute(conn, sql, verbose=verbose)


def main():
    engine: Engine = create_engine(
        connstr(),
        poolclass=NullPool,
        execution_options={"isolation_level": "AUTOCOMMIT"},
    )

    configs = [
        Config(expt_name="default"),
    ]

    pbar = tqdm(range(len(configs)), desc="Configs.", leave=None)
    for config in configs:
        time.time()
        pbar.set_description(f"Config: {config.expt_name} {connstr()}")
        try:
            with engine.connect() as conn:
                dsb(engine, conn, config)
        except Exception:
            traceback.print_exc()
            print(f"ERROR FOR CONFIG: {config.expt_name}")
            pass
        pbar.update()
    pbar.close()


if __name__ == "__main__":
    main()
