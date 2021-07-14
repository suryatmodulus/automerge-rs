use crate::error::DecodeError;
use crate::{
    ActorId, Change, ChangeHash, CursorDiff, Diff, DiffEdit, ElementId, Key, ListDiff, MapDiff,
    MultiElementInsert, ObjType, ObjectId, Op, OpId, OpType, Patch, RootDiff, ScalarValue,
    ScalarValues, TableDiff, TextDiff,
};
use rmpv::Value;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::format;
use std::hash::Hash;
use std::num::NonZeroU32;
use std::str::FromStr;

pub trait Packable {
    fn pack(&self, buffer: &mut Vec<u8>);
}

impl Packable for ChangeHash {
    fn pack(&self, buffer: &mut Vec<u8>) {
        rmp::encode::write_bin(buffer, &self.0).unwrap();
    }
}

impl<T> Packable for &[T]
where
    T: Packable,
{
    fn pack(&self, buffer: &mut Vec<u8>) {
        rmp::encode::write_array_len(buffer, self.len() as u32).unwrap();
        for element in *self {
            element.pack(buffer)
        }
    }
}

fn msgpack_datatype(value: &ScalarValue) -> Option<&'static str> {
    match value {
        ScalarValue::Counter(_) => Some("counter"),
        ScalarValue::Timestamp(_) => Some("timestamp"),
        ScalarValue::Cursor(_) => Some("cursor"),
        ScalarValue::Int(_) => Some("int"),
        ScalarValue::Uint(_) => Some("uint"),
        _ => None,
    }
}

impl Packable for ScalarValue {
    fn pack(&self, buffer: &mut Vec<u8>) {
        match self {
            ScalarValue::Bytes(v) => rmp::encode::write_bin(buffer, v).unwrap(),
            ScalarValue::Str(v) => rmp::encode::write_str(buffer, v).unwrap(),
            ScalarValue::Int(v) => rmp::encode::write_i64(buffer, *v).unwrap(),
            ScalarValue::Uint(v) => rmp::encode::write_u64(buffer, *v).unwrap(),
            ScalarValue::F64(v) => rmp::encode::write_f64(buffer, *v).unwrap(),
            ScalarValue::Counter(v) => rmp::encode::write_i64(buffer, *v).unwrap(),
            ScalarValue::Timestamp(v) => rmp::encode::write_i64(buffer, *v).unwrap(),
            ScalarValue::Cursor(v) => {
                rmp::encode::write_str(buffer, v.to_string().as_str()).unwrap()
            }
            ScalarValue::Boolean(v) => rmp::encode::write_bool(buffer, *v).unwrap(),
            ScalarValue::Null => rmp::encode::write_nil(buffer).unwrap(),
        }
    }
}

impl<T> Packable for Vec<T>
where
    T: Packable,
{
    fn pack(&self, buffer: &mut Vec<u8>) {
        self.as_slice().pack(buffer)
    }
}

impl Packable for OpId {
    fn pack(&self, buffer: &mut Vec<u8>) {
        rmp::encode::write_str(buffer, self.to_string().as_str()).unwrap();
    }
}

impl Packable for ObjectId {
    fn pack(&self, buffer: &mut Vec<u8>) {
        rmp::encode::write_str(buffer, self.to_string().as_str()).unwrap();
    }
}

impl Packable for Key {
    fn pack(&self, buffer: &mut Vec<u8>) {
        rmp::encode::write_str(buffer, self.to_string().as_str()).unwrap();
    }
}

impl Packable for ElementId {
    fn pack(&self, buffer: &mut Vec<u8>) {
        rmp::encode::write_str(buffer, self.to_string().as_str()).unwrap();
    }
}

impl Packable for Change {
    fn pack(&self, buffer: &mut Vec<u8>) {
        let mut len = 6;

        if self.message.is_some() {
            len += 1
        }

        if self.hash.is_some() {
            len += 1
        }

        if !self.extra_bytes.is_empty() {
            len += 1
        }

        rmp::encode::write_map_len(buffer, len).unwrap();

        rmp::encode::write_str(buffer, "actor").unwrap();
        rmp::encode::write_bin(buffer, &self.actor_id.0).unwrap();

        rmp::encode::write_str(buffer, "deps").unwrap();
        self.deps.pack(buffer);

        rmp::encode::write_str(buffer, "seq").unwrap();
        rmp::encode::write_u64(buffer, self.seq).unwrap();

        rmp::encode::write_str(buffer, "startOp").unwrap();
        rmp::encode::write_u64(buffer, self.start_op).unwrap();

        rmp::encode::write_str(buffer, "time").unwrap();
        rmp::encode::write_i64(buffer, self.time).unwrap();

        if let Some(hash) = self.hash {
            rmp::encode::write_str(buffer, "hash").unwrap();
            rmp::encode::write_bin(buffer, &hash.0).unwrap();
        }

        if let Some(message) = &self.message {
            rmp::encode::write_str(buffer, "message").unwrap();
            rmp::encode::write_str(buffer, message).unwrap();
        }

        rmp::encode::write_str(buffer, "ops").unwrap();
        self.operations.pack(buffer);

        if !self.extra_bytes.is_empty() {
            rmp::encode::write_str(buffer, "extra_bytes").unwrap();
            rmp::encode::write_bin(buffer, &self.extra_bytes).unwrap();
        }
    }
}

impl Packable for Op {
    fn pack(&self, buffer: &mut Vec<u8>) {
        rmp::encode::write_map_len(buffer, 5).unwrap();

        rmp::encode::write_str(buffer, "action").unwrap();
        match &self.action {
            OpType::Make(ObjType::Map) => rmp::encode::write_str(buffer, "makeMap").unwrap(),
            OpType::Make(ObjType::List) => rmp::encode::write_str(buffer, "makeList").unwrap(),
            OpType::Make(ObjType::Table) => rmp::encode::write_str(buffer, "makeTable").unwrap(),
            OpType::Make(ObjType::Text) => rmp::encode::write_str(buffer, "makeText").unwrap(),
            OpType::Set(value) => {
                if let Some(datatype) = msgpack_datatype(value) {
                    rmp::encode::write_array_len(buffer, 3).unwrap();
                    rmp::encode::write_str(buffer, "set").unwrap();
                    value.pack(buffer);
                    rmp::encode::write_str(buffer, datatype).unwrap();
                } else {
                    rmp::encode::write_array_len(buffer, 2).unwrap();
                    rmp::encode::write_str(buffer, "set").unwrap();
                    value.pack(buffer);
                }
            }
            OpType::Del(num) => {
                let num: u32 = (*num).into();
                if num > 1 {
                    rmp::encode::write_array_len(buffer, 2).unwrap();
                    rmp::encode::write_str(buffer, "del").unwrap();
                    rmp::encode::write_u32(buffer, num).unwrap();
                } else {
                    rmp::encode::write_str(buffer, "del").unwrap();
                }
            }
            OpType::Inc(i) => {
                rmp::encode::write_array_len(buffer, 2).unwrap();
                rmp::encode::write_str(buffer, "inc").unwrap();
                rmp::encode::write_i64(buffer, *i).unwrap();
            }
            OpType::MultiSet(values) => {
                // will crash is values is length 0 - should never happen
                if let Some(datatype) = msgpack_datatype(&values.get(0).unwrap()) {
                    rmp::encode::write_array_len(buffer, 3).unwrap();
                    rmp::encode::write_str(buffer, "mset").unwrap();
                    values.vec.pack(buffer);
                    rmp::encode::write_str(buffer, datatype).unwrap();
                } else {
                    rmp::encode::write_array_len(buffer, 2).unwrap();
                    rmp::encode::write_str(buffer, "mset").unwrap();
                    values.vec.pack(buffer);
                }
            }
        }

        rmp::encode::write_str(buffer, "obj").unwrap();
        self.obj.pack(buffer);

        if self.key.is_map_key() {
            rmp::encode::write_str(buffer, "key").unwrap();
        } else {
            rmp::encode::write_str(buffer, "elemId").unwrap();
        }
        self.key.pack(buffer);

        rmp::encode::write_str(buffer, "pred").unwrap();
        self.pred.0.pack(buffer);

        rmp::encode::write_str(buffer, "insert").unwrap();
        rmp::encode::write_bool(buffer, self.insert).unwrap();
    }
}

/*
pub struct Patch {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub actor: Option<ActorId>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub seq: Option<u64>,
    pub clock: HashMap<ActorId, u64>,
    pub deps: Vec<ChangeHash>,
    pub max_op: u64,
    pub pending_changes: usize,
    pub diffs: RootDiff,
}
*/

impl Packable for Patch {
    fn pack(&self, buffer: &mut Vec<u8>) {
        let len = 5 + self.actor.is_some() as u32 + self.seq.is_some() as u32;
        rmp::encode::write_map_len(buffer, len).unwrap();

        if let Some(actor) = &self.actor {
            rmp::encode::write_str(buffer, "actor").unwrap();
            rmp::encode::write_bin(buffer, &actor.0).unwrap();
        }

        if let Some(seq) = &self.seq {
            rmp::encode::write_str(buffer, "seq").unwrap();
            rmp::encode::write_u64(buffer, *seq).unwrap();
        }

        rmp::encode::write_str(buffer, "clock").unwrap();
        rmp::encode::write_map_len(buffer, self.clock.len() as u32).unwrap();
        for (key, value) in &self.clock {
            rmp::encode::write_bin(buffer, &key.0).unwrap();
            rmp::encode::write_u64(buffer, *value).unwrap();
        }

        rmp::encode::write_str(buffer, "deps").unwrap();
        self.deps.pack(buffer);

        rmp::encode::write_str(buffer, "maxOp").unwrap();
        rmp::encode::write_u64(buffer, self.max_op).unwrap();

        rmp::encode::write_str(buffer, "pending").unwrap();
        rmp::encode::write_u64(buffer, self.pending_changes as u64).unwrap();

        rmp::encode::write_str(buffer, "diffs").unwrap();
        self.diffs.pack(buffer);
    }
}

/*
    Map(MapDiff),
    Table(TableDiff),
    List(ListDiff),
    Text(TextDiff),
    Value(ScalarValue),
    Cursor(CursorDiff),
*/

impl Packable for RootDiff {
    fn pack(&self, buffer: &mut Vec<u8>) {
        let len = 3;
        rmp::encode::write_map_len(buffer, len).unwrap();

        rmp::encode::write_str(buffer, "type").unwrap();
        rmp::encode::write_str(buffer, "map").unwrap();

        rmp::encode::write_str(buffer, "obj").unwrap();
        rmp::encode::write_str(buffer, "_root").unwrap();

        rmp::encode::write_str(buffer, "props").unwrap();
        self.props.pack(buffer);
    }
}

impl Packable for Diff {
    fn pack(&self, buffer: &mut Vec<u8>) {
        match self {
            Diff::Map(diff) => diff.pack(buffer),
            Diff::Table(diff) => diff.pack(buffer),
            Diff::List(diff) => diff.pack(buffer),
            Diff::Text(diff) => diff.pack(buffer),
            Diff::Value(value) => {
                let datatype = msgpack_datatype(value);
                let len = 2 + datatype.is_some() as u32;
                rmp::encode::write_map_len(buffer, len).unwrap();

                rmp::encode::write_str(buffer, "type").unwrap();
                rmp::encode::write_str(buffer, "value").unwrap();

                rmp::encode::write_str(buffer, "value").unwrap();
                value.pack(buffer);

                if let Some(d) = datatype {
                    rmp::encode::write_str(buffer, "datatype").unwrap();
                    rmp::encode::write_str(buffer, d).unwrap();
                }
            }
            Diff::Cursor(diff) => diff.pack(buffer),
        }
    }
}

impl Packable for HashMap<SmolStr, HashMap<OpId, Diff>> {
    fn pack(&self, buffer: &mut Vec<u8>) {
        rmp::encode::write_map_len(buffer, self.len() as u32).unwrap();
        for (prop, conflicts) in self {
            rmp::encode::write_str(buffer, prop).unwrap();
            rmp::encode::write_map_len(buffer, conflicts.len() as u32).unwrap();
            for (opid, value) in conflicts {
                opid.pack(buffer);
                value.pack(buffer);
            }
        }
    }
}

impl Packable for MapDiff {
    fn pack(&self, buffer: &mut Vec<u8>) {
        let len = 3;
        rmp::encode::write_map_len(buffer, len).unwrap();

        rmp::encode::write_str(buffer, "type").unwrap();
        rmp::encode::write_str(buffer, "map").unwrap();

        rmp::encode::write_str(buffer, "obj").unwrap();
        self.object_id.pack(buffer);

        rmp::encode::write_str(buffer, "props").unwrap();
        self.props.pack(buffer);
    }
}

impl Packable for ListDiff {
    fn pack(&self, buffer: &mut Vec<u8>) {
        let len = 3;
        rmp::encode::write_map_len(buffer, len).unwrap();

        rmp::encode::write_str(buffer, "type").unwrap();
        rmp::encode::write_str(buffer, "list").unwrap();

        rmp::encode::write_str(buffer, "obj").unwrap();
        self.object_id.pack(buffer);

        rmp::encode::write_str(buffer, "edits").unwrap();
        rmp::encode::write_array_len(buffer, self.edits.len() as u32).unwrap();
        for edit in &self.edits {
            edit.pack(buffer);
        }
    }
}

impl Packable for TableDiff {
    fn pack(&self, buffer: &mut Vec<u8>) {
        let len = 3;
        rmp::encode::write_map_len(buffer, len).unwrap();

        rmp::encode::write_str(buffer, "type").unwrap();
        rmp::encode::write_str(buffer, "table").unwrap();

        rmp::encode::write_str(buffer, "obj").unwrap();
        self.object_id.pack(buffer);

        rmp::encode::write_str(buffer, "props").unwrap();
        self.props.pack(buffer);
    }
}

impl Packable for TextDiff {
    fn pack(&self, buffer: &mut Vec<u8>) {
        let len = 3;
        rmp::encode::write_map_len(buffer, len).unwrap();

        rmp::encode::write_str(buffer, "type").unwrap();
        rmp::encode::write_str(buffer, "text").unwrap();

        rmp::encode::write_str(buffer, "obj").unwrap();
        self.object_id.pack(buffer);

        rmp::encode::write_str(buffer, "edits").unwrap();
        rmp::encode::write_array_len(buffer, self.edits.len() as u32).unwrap();
        for edit in &self.edits {
            edit.pack(buffer);
        }
    }
}

impl Packable for CursorDiff {
    fn pack(&self, buffer: &mut Vec<u8>) {
        let len = 4;
        rmp::encode::write_map_len(buffer, len).unwrap();

        rmp::encode::write_str(buffer, "type").unwrap();
        rmp::encode::write_str(buffer, "cursor").unwrap();

        rmp::encode::write_str(buffer, "obj").unwrap();
        self.object_id.pack(buffer);

        rmp::encode::write_str(buffer, "elem").unwrap();
        self.elem_id.pack(buffer);

        rmp::encode::write_str(buffer, "index").unwrap();
        rmp::encode::write_u32(buffer, self.index).unwrap();
    }
}

impl Packable for DiffEdit {
    fn pack(&self, buffer: &mut Vec<u8>) {
        match self {
            DiffEdit::SingleElementInsert {
                index,
                elem_id,
                op_id,
                value,
            } => {
                let len = 5;
                rmp::encode::write_map_len(buffer, len).unwrap();

                rmp::encode::write_str(buffer, "action").unwrap();
                rmp::encode::write_str(buffer, "insert").unwrap();

                rmp::encode::write_str(buffer, "index").unwrap();
                rmp::encode::write_u64(buffer, *index).unwrap();

                rmp::encode::write_str(buffer, "elemId").unwrap();
                elem_id.pack(buffer);

                rmp::encode::write_str(buffer, "opId").unwrap();
                op_id.pack(buffer);

                rmp::encode::write_str(buffer, "value").unwrap();
                value.pack(buffer);
            }
            DiffEdit::Update {
                index,
                op_id,
                value,
            } => {
                rmp::encode::write_map_len(buffer, 4).unwrap();

                rmp::encode::write_str(buffer, "action").unwrap();
                rmp::encode::write_str(buffer, "update").unwrap();

                rmp::encode::write_str(buffer, "index").unwrap();
                rmp::encode::write_u64(buffer, *index).unwrap();

                rmp::encode::write_str(buffer, "opId").unwrap();
                op_id.pack(buffer);

                rmp::encode::write_str(buffer, "value").unwrap();
                value.pack(buffer);
            }
            DiffEdit::MultiElementInsert(multi) => {
                let datatype = multi.values.get(0).and_then(|v| msgpack_datatype(&v));
                let len = 4 + datatype.is_some() as u32;
                rmp::encode::write_map_len(buffer, len).unwrap();

                rmp::encode::write_str(buffer, "action").unwrap();
                rmp::encode::write_str(buffer, "multi-insert").unwrap();

                rmp::encode::write_str(buffer, "index").unwrap();
                rmp::encode::write_u64(buffer, multi.index).unwrap();

                rmp::encode::write_str(buffer, "elemId").unwrap();
                multi.elem_id.pack(buffer);

                rmp::encode::write_str(buffer, "values").unwrap();
                multi.values.vec.pack(buffer);

                if let Some(d) = datatype {
                    rmp::encode::write_str(buffer, "datatype").unwrap();
                    rmp::encode::write_str(buffer, d).unwrap();
                }
            }
            DiffEdit::Remove { index, count } => {
                rmp::encode::write_map_len(buffer, 3).unwrap();

                rmp::encode::write_str(buffer, "action").unwrap();
                rmp::encode::write_str(buffer, "remove").unwrap();

                rmp::encode::write_str(buffer, "index").unwrap();
                rmp::encode::write_u64(buffer, *index).unwrap();

                rmp::encode::write_str(buffer, "count").unwrap();
                rmp::encode::write_u64(buffer, *count).unwrap();
            }
        }
    }
}

// I need a local version of the struct to implement try_from
struct MPVal<'a>(&'a Value);

fn v_get<'a, T>(props: &'a [(Value, Value)], key: &str) -> Result<T, DecodeError>
where
    T: TryFrom<MPVal<'a>, Error = DecodeError>,
{
    let v = v_find(props, key).ok_or_else(|| DecodeError(format!("no such property {}", key)))?;
    MPVal(v).try_into()
}

fn v_get_value(
    props: &[(Value, Value)],
    key: &str,
    datatype: Option<String>,
) -> Result<ScalarValue, DecodeError> {
    let v = v_find(props, key).ok_or_else(|| DecodeError(format!("no such property {}", key)))?;
    match datatype.as_deref() {
        Some("int") => {
            let n = MPVal(v).try_into()?;
            Ok(ScalarValue::Int(n))
        }
        Some("uint") => {
            let n = MPVal(v).try_into()?;
            Ok(ScalarValue::Uint(n))
        }
        Some("counter") => {
            let n = MPVal(v).try_into()?;
            Ok(ScalarValue::Counter(n))
        }
        Some("timestamp") => {
            let n = MPVal(v).try_into()?;
            Ok(ScalarValue::Timestamp(n))
        }
        Some("cursor") => {
            let n = MPVal(v).try_into()?;
            Ok(ScalarValue::Cursor(n))
        }
        Some(other) => Err(DecodeError(format!("no such datatype {}", other))),
        None => Ok(MPVal(v).try_into()?),
    }
}

fn v_get_values(
    props: &[(Value, Value)],
    key: &str,
    datatype: Option<String>,
) -> Result<ScalarValues, DecodeError> {
    let v = v_find(props, key).ok_or_else(|| DecodeError(format!("no such property {}", key)))?;
    match datatype.as_deref() {
        Some("int") => {
            let values: Vec<i64> = MPVal(v).try_into()?;
            let values: Vec<_> = values.into_iter().map(ScalarValue::Int).collect();
            Ok(values.try_into().unwrap())
        }
        Some("uint") => {
            let values: Vec<u64> = MPVal(v).try_into()?;
            let values: Vec<_> = values.into_iter().map(ScalarValue::Uint).collect();
            Ok(values.try_into().unwrap())
        }
        Some("counter") => {
            let values: Vec<i64> = MPVal(v).try_into()?;
            let values: Vec<_> = values
                .into_iter()
                .map(ScalarValue::Counter)
                .collect();
            Ok(values.try_into().unwrap())
        }
        Some("timestamp") => {
            let values: Vec<i64> = MPVal(v).try_into()?;
            let values: Vec<_> = values
                .into_iter()
                .map(ScalarValue::Timestamp)
                .collect();
            Ok(values.try_into().unwrap())
        }
        Some("cursor") => {
            let values: Vec<OpId> = MPVal(v).try_into()?;
            let values: Vec<_> = values.into_iter().map(ScalarValue::Cursor).collect();
            Ok(values.try_into().unwrap())
        }
        Some(other) => Err(DecodeError(format!("no such datatype {}", other))),
        None => {
            let values: Vec<ScalarValue> = MPVal(v).try_into()?;
            Ok(values.try_into().unwrap())
        }
    }
}

fn v_get_opt<'a, T>(props: &'a [(Value, Value)], key: &str) -> Result<Option<T>, DecodeError>
where
    T: TryFrom<MPVal<'a>, Error = DecodeError>,
{
    v_find(props, key).map(|v| MPVal(v).try_into()).transpose()
}

fn v_find<'a>(props: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    let key = Value::String(key.into());
    props
        .iter()
        .find_map(|(prop, val)| if prop == &key { Some(val) } else { None })
}

impl TryFrom<MPVal<'_>> for Op {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        if let MPVal(Value::Map(props)) = value {
            let action = v_get(props, "action")?;
            let obj = v_get(props, "obj")?;
            let pred: Vec<OpId> = v_get(props, "pred")?;
            let insert = v_get(props, "insert")?;

            let elem_id: Option<ElementId> = v_get_opt(props, "elemId")?;
            let key: Option<String> = v_get_opt(props, "key")?;
            let key: Option<Key> = key
                .map(|k| Key::Map(k.into()))
                .or_else(|| elem_id.map(Key::Seq));
            let key = key.ok_or_else(|| DecodeError("op missing key and elemId".into()))?;

            Ok(Op {
                action,
                obj,
                key,
                pred: pred.into(),
                insert,
            })
        } else {
            Err(DecodeError("op from non map".into()))
        }
    }
}

impl TryFrom<MPVal<'_>> for ActorId {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        match value {
            MPVal(Value::Binary(bin)) => Ok(ActorId(bin.clone())),
            _ => Err(DecodeError("ActorId not binary".into())),
        }
    }
}

impl TryFrom<MPVal<'_>> for i64 {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        match value {
            MPVal(Value::Integer(val)) => val
                .as_i64()
                .ok_or_else(|| DecodeError("invalid i64".into())),
            _ => Err(DecodeError("invalid i64".into())),
        }
    }
}

impl<'a, T> TryFrom<MPVal<'a>> for Vec<T>
where
    T: TryFrom<MPVal<'a>, Error = DecodeError>,
{
    type Error = DecodeError;
    fn try_from(value: MPVal<'a>) -> Result<Self, Self::Error> {
        if let MPVal(Value::Array(vals)) = value {
            vals.iter().map(|v| MPVal(v).try_into()).collect()
        } else {
            Err(DecodeError("not an array".to_string()))
        }
    }
}

impl<'a, T, U> TryFrom<MPVal<'a>> for HashMap<T, U>
where
    T: TryFrom<MPVal<'a>, Error = DecodeError>,
    T: Hash,
    T: Eq,
    U: TryFrom<MPVal<'a>, Error = DecodeError>,
{
    type Error = DecodeError;

    fn try_from(value: MPVal<'a>) -> Result<Self, Self::Error> {
        if let MPVal(Value::Map(vals)) = value {
            vals.iter()
                .map(|(key, val)| Ok((MPVal(key).try_into()?, MPVal(val).try_into()?)))
                .collect()
        } else {
            Err(DecodeError("not a map".to_string()))
        }
    }
}

impl TryFrom<MPVal<'_>> for ScalarValue {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        match value {
            MPVal(Value::String(_)) => {
                let s: String = value.try_into()?;
                Ok(ScalarValue::Str(s.into()))
            }
            MPVal(Value::Integer(val)) => {
                if val.is_u64() {
                    let i = val
                        .as_u64()
                        .ok_or_else(|| DecodeError("invalid u64".into()))?;
                    Ok(ScalarValue::Uint(i))
                } else {
                    let i = val
                        .as_i64()
                        .ok_or_else(|| DecodeError("invalid i64".into()))?;
                    Ok(ScalarValue::Int(i))
                }
            }
            MPVal(Value::F64(val)) => Ok(ScalarValue::F64(*val)),
            MPVal(Value::Binary(val)) => Ok(ScalarValue::Bytes(val.clone())),
            MPVal(Value::Boolean(val)) => Ok(ScalarValue::Boolean(*val)),
            MPVal(Value::Nil) => Ok(ScalarValue::Null),
            _ => Err(DecodeError("invalid scalarvalue".into())),
        }
    }
}

impl TryFrom<MPVal<'_>> for u64 {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        match value {
            MPVal(Value::Integer(val)) => val
                .as_u64()
                .ok_or_else(|| DecodeError("invalid u64".into())),
            _ => Err(DecodeError("invalid u64".into())),
        }
    }
}

impl TryFrom<MPVal<'_>> for usize {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        match value {
            MPVal(Value::Integer(val)) => val
                .as_u64()
                .map(|v| v as usize)
                .ok_or_else(|| DecodeError("invalid u64".into())),
            _ => Err(DecodeError("invalid u64".into())),
        }
    }
}

impl TryFrom<MPVal<'_>> for SmolStr {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        match value {
            MPVal(Value::String(val)) => val
                .clone()
                .into_str()
                .map(|s| s.into())
                .ok_or_else(|| DecodeError("invalid utf8 string".into())),
            _ => Err(DecodeError("invalid string".into())),
        }
    }
}

impl TryFrom<MPVal<'_>> for String {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        match value {
            MPVal(Value::String(val)) => val
                .clone()
                .into_str()
                .ok_or_else(|| DecodeError("invalid utf8 string".into())),
            _ => Err(DecodeError("invalid string".into())),
        }
    }
}

impl TryFrom<MPVal<'_>> for OpId {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        let val: String = value.try_into()?;
        OpId::from_str(&val).map_err(|_| DecodeError("invalid opid format".into()))
    }
}

impl TryFrom<MPVal<'_>> for ObjectId {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        let val: String = value.try_into()?;
        ObjectId::from_str(&val).map_err(|_| DecodeError("invalid opid format".into()))
    }
}

impl TryFrom<MPVal<'_>> for ElementId {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        let val: String = value.try_into()?;
        ElementId::from_str(&val).map_err(|_| DecodeError("invalid opid format".into()))
    }
}

impl TryFrom<MPVal<'_>> for RootDiff {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        if let MPVal(Value::Map(root)) = value {
            let props = v_get(root, "props")?;
            Ok(RootDiff { props })
        } else {
            Err(DecodeError("root_diff from non map".into()))
        }
    }
}

impl TryFrom<MPVal<'_>> for DiffEdit {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        if let MPVal(Value::Map(root)) = value {
            let action: String = v_get(root, "action")?;
            match action.as_str() {
                "insert" => {
                    let index = v_get(root, "index")?;
                    let op_id = v_get(root, "opId")?;
                    let elem_id = v_get(root, "elemId")?;
                    let value = v_get(root, "value")?;
                    Ok(DiffEdit::SingleElementInsert {
                        index,
                        elem_id,
                        op_id,
                        value,
                    })
                }
                "update" => {
                    let index = v_get(root, "index")?;
                    let op_id = v_get(root, "op_id")?;
                    let value = v_get(root, "value")?;
                    Ok(DiffEdit::Update {
                        index,
                        op_id,
                        value,
                    })
                }
                "remove" => {
                    let index = v_get(root, "index")?;
                    let count = v_get(root, "count")?;
                    Ok(DiffEdit::Remove { index, count })
                }
                "multi-insert" => {
                    let index = v_get(root, "index")?;
                    let elem_id = v_get(root, "elemId")?;
                    let datatype = v_get_opt(root, "datatype")?;
                    let values = v_get_values(root, "values", datatype)?;
                    Ok(DiffEdit::MultiElementInsert(MultiElementInsert {
                        index,
                        elem_id,
                        values,
                    }))
                }
                other => Err(DecodeError(format!("diff-edit type {} unknown", other))),
            }
        } else {
            Err(DecodeError("diff-edit from non map".into()))
        }
    }
}

impl TryFrom<MPVal<'_>> for Diff {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        if let MPVal(Value::Map(root)) = value {
            let kind: String = v_get(root, "type")?;
            match kind.as_str() {
                "map" => {
                    let object_id = v_get(root, "obj")?;
                    let props = v_get(root, "props")?;
                    Ok(Diff::Map(MapDiff { object_id, props }))
                }
                "table" => {
                    let object_id = v_get(root, "obj")?;
                    let props = v_get(root, "props")?;
                    Ok(Diff::Table(TableDiff { object_id, props }))
                }
                "list" => {
                    let object_id = v_get(root, "obj")?;
                    let edits = v_get(root, "edits")?;
                    Ok(Diff::List(ListDiff { object_id, edits }))
                }
                "text" => {
                    let object_id = v_get(root, "obj")?;
                    let edits = v_get(root, "edits")?;
                    Ok(Diff::Text(TextDiff { object_id, edits }))
                }
                "value" => {
                    let datatype = v_get_opt(root, "datatype")?;
                    let value = v_get_value(root, "value", datatype)?;
                    Ok(Diff::Value(value))
                }
                other => Err(DecodeError(format!("diff type {} unknown", other))),
            }
        } else {
            Err(DecodeError("diff from non map".into()))
        }
    }
}

impl TryFrom<MPVal<'_>> for OpType {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        match value {
            MPVal(Value::String(val)) => {
                let val = val
                    .as_str()
                    .ok_or_else(|| DecodeError("invalid utf8 string".into()))?;
                match val {
                    "makeMap" => Ok(OpType::Make(ObjType::Map)),
                    "makeList" => Ok(OpType::Make(ObjType::List)),
                    "makeText" => Ok(OpType::Make(ObjType::Text)),
                    "makeTable" => Ok(OpType::Make(ObjType::Table)),
                    "del" => Ok(OpType::Del(NonZeroU32::new(1).unwrap())),
                    _ => Err(DecodeError(format!("invalid action {}", val))),
                }
            }
            MPVal(Value::Array(vals)) => match vals.len() {
                2 => {
                    let action: String = MPVal(&vals[0]).try_into()?;
                    match action.as_str() {
                        "del" => {
                            let num: u64 = MPVal(&vals[1]).try_into()?;
                            Ok(OpType::Del(NonZeroU32::new(num as u32).unwrap()))
                        }
                        "inc" => {
                            let num: i64 = MPVal(&vals[1]).try_into()?;
                            Ok(OpType::Inc(num))
                        }
                        "set" => {
                            let val: ScalarValue = MPVal(&vals[1]).try_into()?;
                            Ok(OpType::Set(val))
                        }
                        "mset" => {
                            let vals: Vec<ScalarValue> = MPVal(&vals[1]).try_into()?;
                            Ok(OpType::MultiSet(vals.try_into().map_err(|_| {
                                DecodeError("Cant decode ScalarValues".into())
                            })?))
                        }
                        action => Err(DecodeError(format!("invalid action {}", action))),
                    }
                }
                3 => {
                    let action: String = MPVal(&vals[0]).try_into()?;
                    let datatype: String = MPVal(&vals[2]).try_into()?;
                    match (action.as_str(), datatype.as_str()) {
                        ("set", "timestamp") => {
                            let value = MPVal(&vals[1]).try_into()?;
                            let value = ScalarValue::Timestamp(value);
                            Ok(OpType::Set(value))
                        }
                        ("mset", "timestamp") => {
                            let values: Vec<i64> = MPVal(&vals[1]).try_into()?;
                            let values: Vec<ScalarValue> = values
                                .into_iter()
                                .map(ScalarValue::Timestamp)
                                .collect();
                            Ok(OpType::MultiSet(values.try_into().map_err(|_| {
                                DecodeError("Cant decode ScalarValues".into())
                            })?))
                        }
                        ("set", "counter") => {
                            let value = MPVal(&vals[1]).try_into()?;
                            let value = ScalarValue::Counter(value);
                            Ok(OpType::Set(value))
                        }
                        ("mset", "counter") => {
                            let values: Vec<i64> = MPVal(&vals[1]).try_into()?;
                            let values: Vec<_> = values
                                .into_iter()
                                .map(ScalarValue::Counter)
                                .collect();
                            Ok(OpType::MultiSet(values.try_into().map_err(|_| {
                                DecodeError("Cant decode ScalarValues".into())
                            })?))
                        }
                        ("set", "int") => {
                            let value = MPVal(&vals[1]).try_into()?;
                            let value = ScalarValue::Int(value);
                            Ok(OpType::Set(value))
                        }
                        ("mset", "int") => {
                            let values: Vec<i64> = MPVal(&vals[1]).try_into()?;
                            let values: Vec<_> =
                                values.into_iter().map(ScalarValue::Int).collect();
                            Ok(OpType::MultiSet(values.try_into().map_err(|_| {
                                DecodeError("Cant decode ScalarValues".into())
                            })?))
                        }
                        ("set", "uint") => {
                            let value = MPVal(&vals[1]).try_into()?;
                            let value = ScalarValue::Uint(value);
                            Ok(OpType::Set(value))
                        }
                        ("mset", "uint") => {
                            let values: Vec<u64> = MPVal(&vals[1]).try_into()?;
                            let values: Vec<_> =
                                values.into_iter().map(ScalarValue::Uint).collect();
                            Ok(OpType::MultiSet(values.try_into().map_err(|_| {
                                DecodeError("Cant decode ScalarValues".into())
                            })?))
                        }
                        ("set", "cursor") => {
                            let value: OpId = MPVal(&vals[1]).try_into()?;
                            let value = ScalarValue::Cursor(value);
                            Ok(OpType::Set(value))
                        }
                        ("mset", "cursor") => {
                            let values: Vec<OpId> = MPVal(&vals[1]).try_into()?;
                            let values: Vec<_> = values
                                .into_iter()
                                .map(ScalarValue::Cursor)
                                .collect();
                            Ok(OpType::MultiSet(values.try_into().map_err(|_| {
                                DecodeError("Cant decode ScalarValues".into())
                            })?))
                        }
                        (action, datatype) => Err(DecodeError(format!(
                            "invalid action/datatype ({},{})",
                            action, datatype
                        ))),
                    }
                }
                _ => Err(DecodeError(format!("invalid action len {}", vals.len()))),
            },
            _ => Err(DecodeError("invalid action type".into())),
        }
    }
}

impl TryFrom<MPVal<'_>> for ChangeHash {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        match value {
            MPVal(Value::Binary(val)) => {
                let array = val.clone().try_into().map_err(|_| {
                    DecodeError(format!("invalid change hash length {}", val.len()))
                })?;
                Ok(ChangeHash(array))
            }
            _ => Err(DecodeError("invalid binary".into())),
        }
    }
}

impl TryFrom<MPVal<'_>> for Vec<u8> {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        match value {
            MPVal(Value::Binary(val)) => Ok(val.clone()),
            _ => Err(DecodeError("invalid binary".into())),
        }
    }
}

impl TryFrom<MPVal<'_>> for bool {
    type Error = DecodeError;

    fn try_from(value: MPVal<'_>) -> Result<Self, Self::Error> {
        match value {
            MPVal(Value::Boolean(val)) => Ok(*val),
            _ => Err(DecodeError("invalid bool".into())),
        }
    }
}

impl TryFrom<Value> for Change {
    type Error = DecodeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if let Value::Map(props) = &value {
            let operations = v_get(props, "ops")?;
            let actor_id = v_get(props, "actor")?;
            let hash = v_get_opt(props, "hash")?;
            let message = v_get_opt(props, "message")?;
            let seq = v_get(props, "seq")?;
            let start_op = v_get(props, "startOp")?;
            let time = v_get(props, "time")?;
            let deps = v_get(props, "deps")?;
            let extra_bytes: Option<Vec<u8>> = v_get_opt(props, "extra_bytes")?;
            let extra_bytes = extra_bytes.unwrap_or_default();
            Ok(Change {
                operations,
                actor_id,
                hash,
                seq,
                start_op,
                time,
                message,
                deps,
                extra_bytes,
            })
        } else {
            Err(DecodeError("change from non map".into()))
        }
    }
}

impl TryFrom<Value> for Patch {
    type Error = DecodeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if let Value::Map(props) = &value {
            let actor = v_get_opt(props, "actor")?;
            let seq = v_get_opt(props, "seq")?;
            let clock = v_get(props, "clock")?;
            let deps = v_get(props, "deps")?;
            let pending_changes = v_get(props, "pending")?;
            let max_op = v_get(props, "maxOp")?;
            let diffs = v_get(props, "diffs")?;
            Ok(Patch {
                actor,
                seq,
                clock,
                deps,
                max_op,
                pending_changes,
                diffs,
            })
        } else {
            Err(DecodeError("patch from non map".into()))
        }
    }
}

/*
pub fn roundtrip_change<T>(change: &Change) -> Result<(), DecodeError> {
    let mut data = Vec::new();
    change.pack(&mut data);
    let value = rmpv::decode::value::read_value(&mut data.as_slice())
        .map_err(|e| DecodeError(format!("rmpv decode error {:?}", e)))?;
    let result: Change = value.try_into()?;
    assert!(&result == change);
    Ok(())
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ActorId, Change, SortedVec};
    use serde_json;

    #[test]
    fn test_patch_roundtrip_1() {
        //      let json = r#"{"actor":"4b66e7db4036434bba0a10e620036250","seq":1,"startOp":1,"time":0,"deps":[],"ops":[{"action":"makeMap","obj":"_root","key":"birds","pred":[]},{"action":"set","obj":"1@4b66e7db4036434bba0a10e620036250","key":"wrens","datatype":"int","value":3,"pred":[]}]}"#;
        //      let change : Patch = serde_json::from_str(json).unwrap();
        //      crate::roundtrip_patch(&change).unwrap();
    }

    #[test]
    fn test_roundtrip_1() {
        let json = r#"{"actor":"4b66e7db4036434bba0a10e620036250","seq":1,"startOp":1,"time":0,"deps":[],"ops":[{"action":"makeMap","obj":"_root","key":"birds","pred":[]},{"action":"set","obj":"1@4b66e7db4036434bba0a10e620036250","key":"wrens","datatype":"int","value":3,"pred":[]}]}"#;
        let change: Change = serde_json::from_str(json).unwrap();
        crate::roundtrip_change(&change).unwrap();
    }

    #[test]
    fn test_roundtrip_2() {
        let actor = ActorId::random();
        let change1 = crate::Change {
            actor_id: actor.clone(),
            seq: 1,
            start_op: 1,
            time: 0,
            deps: Vec::new(),
            message: None,
            hash: None,
            operations: vec![
                Op {
                    action: crate::OpType::Make(crate::ObjType::List),
                    obj: ObjectId::Root,
                    key: "list".into(),
                    pred: SortedVec::new(),
                    insert: false,
                },
                Op {
                    action: crate::OpType::Set(ScalarValue::Str("something".into())),
                    obj: actor.op_id_at(1).into(),
                    key: crate::ElementId::Head.into(),
                    insert: true,
                    pred: SortedVec::new(),
                },
                Op {
                    action: crate::OpType::Set(ScalarValue::Str("something else".into())),
                    obj: actor.op_id_at(1).into(),
                    key: actor.op_id_at(2).into(),
                    insert: true,
                    pred: SortedVec::new(),
                },
                Op {
                    action: crate::OpType::Set(ScalarValue::Cursor(actor.op_id_at(3))),
                    obj: ObjectId::Root,
                    key: "cursor".into(),
                    insert: false,
                    pred: SortedVec::new(),
                },
            ],
            extra_bytes: Vec::new(),
        };
        let change2 = crate::Change {
            actor_id: actor.clone(),
            seq: 2,
            start_op: 5,
            time: 0,
            deps: vec![],
            message: None,
            hash: None,
            operations: vec![Op {
                action: crate::OpType::Del(NonZeroU32::new(1).unwrap()),
                obj: actor.op_id_at(1).into(),
                key: actor.op_id_at(2).into(),
                insert: false,
                pred: vec![actor.op_id_at(2)].into(),
            }],
            extra_bytes: Vec::new(),
        };
        crate::roundtrip_change(&change1).unwrap();
        crate::roundtrip_change(&change2).unwrap();
    }
}
