package com.datastax.alexott.demos.jdtest1;

import java.io.IOException;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

@SuppressWarnings("serial")
public class ResultSetSerializer extends StdSerializer<ResultSet> {
	public ResultSetSerializer() {
		this(null);
	}

	public ResultSetSerializer(Class<ResultSet> t) {
		super(t);
	}

	void writeItem(Row row, int i, String name, DataType dt, JsonGenerator jgen) throws IOException {
		if (DataType.cboolean().equals(dt)) {
			jgen.writeBooleanField(name, row.getBool(i));
		} else if(DataType.cint().equals(dt)) {
			jgen.writeNumberField(name, row.getInt(i));
		} else {
			jgen.writeStringField(name, row.getObject(i).toString());
		}
	}

	@Override
	public void serialize(ResultSet rs, JsonGenerator jgen, SerializerProvider provider) throws IOException {
		ColumnDefinitions cd = rs.getColumnDefinitions();
		List<ColumnDefinitions.Definition> lcd = cd.asList();
		int lsize = lcd.size();
		String[] names = new String[lsize];
		DataType[] types = new DataType[lsize];
		for (int i = 0; i < lsize; i++) {
			ColumnDefinitions.Definition cdef = lcd.get(i);
			names[i] = cdef.getName();
			types[i] = cdef.getType();
		}

		jgen.writeStartArray();
		for (Row row : rs) {
			jgen.writeStartObject();
			for (int i = 0; i < lsize; i++) {
				String name = names[i];
				if (row.isNull(i)) {
					jgen.writeNullField(name);
				} else {
					writeItem(row, i, name, types[i], jgen);
				}
			}
			jgen.writeEndObject();
		}
		jgen.writeEndArray();
	}

}
