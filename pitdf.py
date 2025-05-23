import pandas as pd
from typing import List, Dict, Optional

class PointInTimeData:
    def __init__(self, key_columns: List[str], value_columns: List[str], default_overlap_mode: str = 'raise', load_path: Optional[str] = None):
        """
        Initializes the PIT store.
        key_columns: list of key fields
        value_columns: list of versioned fields
        """
        self.key_columns = key_columns
        self.value_columns = value_columns
        self.default_overlap_mode = default_overlap_mode
        self.columns = key_columns + ["from_time", "to_time", "change_time"] + value_columns
        if load_path:
            self.load(load_path)
        else:
            self.df = pd.DataFrame(columns=self.columns)
            self.df["from_time"] = pd.to_datetime(self.df["from_time"], utc=True)
            self.df["to_time"] = pd.to_datetime(self.df["to_time"], utc=True)
            self.df["change_time"] = pd.to_datetime(self.df["change_time"], utc=True)
        self.df["from_time"] = pd.to_datetime(self.df["from_time"], utc=True)
        self.df["to_time"] = pd.to_datetime(self.df["to_time"], utc=True)
        self.df["change_time"] = pd.to_datetime(self.df["change_time"], utc=True)

    def _match_key(self, row: Dict) -> pd.Series:
        mask = pd.Series(True, index=self.df.index)
        for col in self.key_columns:
            mask &= self.df[col] == row[col]
        return mask & self.df["to_time"].isna()

    def _check_overlap(self, row: Dict, mode: str = 'raise'):
        from_time = pd.to_datetime(row["from_time"], utc=True)
        key_mask = pd.Series(True, index=self.df.index)
        for col in self.key_columns:
            key_mask &= self.df[col] == row[col]

        overlapping = self.df[key_mask & (
            (self.df["from_time"] < row.get("to_time", pd.Timestamp.max)) &
            ((self.df["to_time"].isna()) | (self.df["to_time"] > from_time))
        )]
        if not overlapping.empty:
            if mode == 'raise':
                raise ValueError("Versioning conflict: overlapping time window.")
            elif mode == 'skip':
                return False
            elif mode == 'replace':
                self.df = self.df.drop(overlapping.index)
            else:
                raise ValueError(f"Invalid overlap handling mode: {mode}")

    def add(self, row: Dict, overlap_mode: Optional[str] = None):
        if overlap_mode is None:
            overlap_mode = self.default_overlap_mode
        row = row.copy()
        row["from_time"] = pd.to_datetime(row["from_time"], utc=True)
        row["to_time"] = pd.NaT
        row["change_time"] = pd.Timestamp.utcnow().tz_localize('UTC')
        if not self._check_overlap(row, mode=overlap_mode): return
        self.df = pd.concat([self.df, pd.DataFrame([row])], ignore_index=True)

    def upsert(self, row: Dict, overlap_mode: Optional[str] = None):
        if overlap_mode is None:
            overlap_mode = self.default_overlap_mode
        row = row.copy()
        from_time = pd.to_datetime(row["from_time"], utc=True)
        mask = self._match_key(row)
        if mask.any():
            self.df.loc[mask, "to_time"] = from_time
        self.add(row, overlap_mode=overlap_mode)

    def batch_upsert(self, records, overlap_mode: Optional[str] = None):
        """
        Insert or update records from a list of dicts or a DataFrame.
        Validates that input contains all required columns.
        """
        if overlap_mode is None:
            overlap_mode = self.default_overlap_mode
        df_in = records if isinstance(records, pd.DataFrame) else pd.DataFrame(records)
        missing_cols = set(self.key_columns + ["from_time"] + self.value_columns) - set(df_in.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns in input: {missing_cols}")
        df_in["from_time"] = pd.to_datetime(df_in["from_time"], utc=True)
        for row in df_in.to_dict(orient="records"):
            self.upsert(row, overlap_mode=overlap_mode)

    def delete(self, key_dict: Dict, delete_time):
        delete_time = pd.to_datetime(delete_time, utc=True)
        mask = pd.Series(True, index=self.df.index)
        for k, v in key_dict.items():
            mask &= self.df[k] == v
        mask &= self.df["to_time"].isna()
        if mask.any():
            self.df.loc[mask, "to_time"] = delete_time
            self.df.loc[mask, "change_time"] = pd.Timestamp.utcnow().tz_localize('UTC')

    def get_active(self, timestamp) -> pd.DataFrame:
        timestamp = pd.to_datetime(timestamp, utc=True)
        return self.df[
            (self.df["from_time"] <= timestamp) &
            ((self.df["to_time"].isna()) | (self.df["to_time"] > timestamp))
        ]

    def latest(self) -> pd.DataFrame:
        return self.df[self.df["to_time"].isna()]

    def full_history(self, key_filter: Optional[Dict] = None) -> pd.DataFrame:
        if key_filter:
            mask = pd.Series(True, index=self.df.index)
            for k, v in key_filter.items():
                mask &= self.df[k] == v
            return self.df[mask].sort_values("from_time")
        return self.df.sort_values(self.key_columns + ["from_time"])

    def snapshot(self, date_range: List) -> pd.DataFrame:
        snapshots = []
        for ts in pd.to_datetime(date_range, utc=True):
            snap = self.get_active(ts).copy()
            snap["snapshot_time"] = ts
            snapshots.append(snap)
        return pd.concat(snapshots).reset_index(drop=True)

    def save(self, path: str):
        self.df.to_parquet(path, index=False)

    def load(self, path: str):
        self.df = pd.read_parquet(path)
        self.df["from_time"] = pd.to_datetime(self.df["from_time"], utc=True)
        self.df["to_time"] = pd.to_datetime(self.df["to_time"], utc=True)
        self.df["change_time"] = pd.to_datetime(self.df["change_time"], utc=True)
