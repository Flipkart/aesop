package com.flipkart.aesop.avro.schemagenerator.main.common;

import java.io.IOException;
import java.util.List;

import jline.Completor;
import jline.ConsoleReader;
import jline.SimpleCompletor;

public class JLineHelper
{
	// console reader
	static ConsoleReader _reader;
	// The autocompletor in the current context
	static Completor _currentCompletor = null;

	/**
	 * JLine helper methods
	 * ===================================================================================
	 */
	public JLineHelper(String prompt)
	{
		try
		{
			JLineHelper._reader = new ConsoleReader();
			_reader.setDefaultPrompt(prompt);
			_reader.setBellEnabled(false);
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Read and return a valid string input
	 * @return the current line read
	 * @throws IOException
	 */
	public String checkAndRead() throws IOException
	{
		String line;
		if ((line = _reader.readLine()) == null)
		{
			System.out.println("Unable to read a valid input from the console");
			return null;
		}

		return line.trim();
	}

	/**
	 * Add a array to autocomplete
	 * @param completorArray The array to add to autocomplete
	 */
	public void addArrayToCompletor(String[] completorArray)
	{
		removeCurrentCompletor();

		if (completorArray == null || completorArray.length == 0)
			return;

		_currentCompletor = new SimpleCompletor(completorArray);
		_reader.addCompletor(_currentCompletor);
	}

	/**
	 * Remove the current auto complete list
	 */
	public void removeCurrentCompletor()
	{
		if (_currentCompletor != null)
			_reader.removeCompletor(_currentCompletor);
		_currentCompletor = null;
	}

	/**
	 * Add a list to autocomplete
	 * @param completorList The list to add to autocomplete
	 */
	public void addListToCompletor(List<String> completorList)
	{

		if (completorList == null || completorList.size() == 0)
			return;

		String completorArray[] = completorList.toArray(new String[completorList.size()]);
		addArrayToCompletor(completorArray);
	}
}
