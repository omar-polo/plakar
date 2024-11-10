PLAKAR(CLEANUP) - CLEANUP (1)

# NAME

**plakar cleanup** - Remove unused data from a Plakar repository

# SYNOPSIS

**plakar cleanup**

# DESCRIPTION

The
**plakar cleanup**
command removes unused blobs, objects, and chunks from a Plakar repository to reduce storage space. It identifies unreferenced data and reorganizes packfiles to ensure only active snapshots and their dependencies are retained. The cleanup process updates snapshot indexes to reflect these changes.

# OPTIONS

No options are available for this command.

# ARGUMENTS

None.

# EXAMPLES

To illustrate usage:

Run cleanup to reclaim storage space:

> > plakar cleanup

# DIAGNOSTICS

The **plakar cleanup** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred during cleanup, such as failure to update indexes or remove data.

# SEE ALSO

plakar(1)

macOS 15.0 - November 3, 2024