ALTER TABLE model_api_connections
 ADD COLUMN max_output_tokens integer
 CHECK (max_output_tokens IS NULL OR max_output_tokens > 0);
