package com.datastax.demos.alexott.btest;

import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "test", name = "btest")
public class BData {
	@PartitionKey
	int id;
	
	@Column(name = "txt")
	String text;
	
	@Column(name = "u")
	private UUID timeUUID;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public UUID getTimeUUID() {
		return timeUUID;
	}

	public void setTimeUUID(UUID timeUUID) {
		this.timeUUID = timeUUID;
	}

	@Override
	public String toString() {
		return "BData [id=" + id + ", text=" + text + ", timeUUID=" + timeUUID + "]";
	}

}
