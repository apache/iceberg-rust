You should implement changes in the neighbouring plan.md file. Implement things step by step. Once you implement a step, run new tests if tests are implemented yet. If not, just skip that. After implementing new features that may impact existing tests, change tests to verify the new feature.
If the step is complete, and I confirmed, you should mark the step complete, with a checkmark in the plan.md next to it.
A step is not always going to be detailed, and will require filling in some gaps. But don't implement more funcitonality that you're not asked to. When in doubt, ask.
Some initial steps may already be partially implemented, check. If you notice it's implementing more than needed, ask.
Sometimes, I'll deny the changes and edit plan.md, and ask you to retry the action with that in mind.
Always annotate where you are in `plan.md`, so that you can continue in next session. Also, add some more details that would help you to continue, i.e. what have you done, what were the reasons. The reasons should be added for humans reading the plan too, it should serve as a description/design of what was done, to the extent that you have information (don't invent reasons).
When a step is more involved, propose a plan to break it down into substeps, and include them into the plan.md if accepted.

Besides this repository, I advise that you reference this pull-request when in doubt: https://github.com/apache/iceberg-rust/pull/1470/files. Note that that PR is not for exactly the same feature, but some parts may be useful.