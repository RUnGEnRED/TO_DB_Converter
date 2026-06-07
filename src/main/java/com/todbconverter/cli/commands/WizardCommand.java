package com.todbconverter.cli.commands;

import com.todbconverter.ui.ConsoleWizard;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

/**
 * Command to launch the interactive configuration wizard.
 */
@Command(name = "wizard", aliases = {"--wizard", "-w"}, description = "Launch interactive configuration wizard")
public class WizardCommand implements Callable<Integer> {

    @Override
    public Integer call() {
        try {
            ConsoleWizard wizard = new ConsoleWizard();
            wizard.run();
            return 0;
        } catch (Exception e) {
            System.err.println("Wizard error: " + e.getMessage());
            e.printStackTrace();
            return 1;
        }
    }
}
