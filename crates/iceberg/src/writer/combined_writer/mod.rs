//! Combined writers compose multiple base writers into a single writer that can handle
//! more complex writing scenarios, such as row-level changes involving both data files and delete files.

pub mod delta_writer;
