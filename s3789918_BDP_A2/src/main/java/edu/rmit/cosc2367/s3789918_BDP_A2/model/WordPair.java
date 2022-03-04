package edu.rmit.cosc2367.s3789918_BDP_A2.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordPair implements WritableComparable<WordPair>{

	private String word;
	private String neighbour;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		word = in.readUTF();
		neighbour = in.readUTF();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeUTF(neighbour);
	}
	
	@Override
	public int compareTo(WordPair o) {
		int difference = this.word.compareTo(o.word);
		if (difference == 0)
			difference = this.neighbour.compareTo(o.neighbour);
		return difference;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public String getNeighbour() {
		return neighbour;
	}

	public void setNeighbour(String neighbour) {
		this.neighbour = neighbour;
	}
	
	@Override
	public String toString() {
		return "( "+word+", "+neighbour+" )";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((word == null) ? 0 : word.hashCode());
		result = prime * result + ((neighbour == null) ? 0 : neighbour.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WordPair other = (WordPair) obj;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		if (neighbour == null) {
			if (other.neighbour != null)
				return false;
		} else if (!neighbour.equals(other.neighbour))
			return false;
		
		return true;
	}
	
	
}

