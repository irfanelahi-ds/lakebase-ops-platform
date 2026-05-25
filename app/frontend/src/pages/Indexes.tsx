import { Box, Typography, Grid, Chip, Card, CardContent, Skeleton, Alert } from "@mui/material";
import { motion } from "framer-motion";
import { useApiData } from "../hooks/useApiData";

const confidenceColor: Record<string, string> = {
  high: "#3FB950",
  medium: "#D29922",
  low: "#8B949E",
};

export default function Indexes() {
  const { data: recs, loading, error } = useApiData<any[]>("/api/indexes/recommendations");

  if (error) {
    return <Alert severity="error" sx={{ mt: 2 }}>Failed to load index recommendations: {error}</Alert>;
  }

  if (loading) {
    return (
      <Box>
        <Skeleton variant="text" width={280} height={40} sx={{ mb: 2 }} />
        <Grid container spacing={2}>
          {Array.from({ length: 6 }).map((_, i) => (
            <Grid item xs={12} sm={6} md={4} key={i}>
              <Skeleton variant="rounded" height={140} />
            </Grid>
          ))}
        </Grid>
      </Box>
    );
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Index Recommendations
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
        AI-generated index recommendations by type and confidence
      </Typography>

      {!recs || recs.length === 0 ? (
        <Card>
          <CardContent>
            <Typography variant="body2" color="text.secondary">
              No data available — the Index Analyzer job has not produced any recommendations yet.
              This page populates with unused, bloated, missing, duplicate, or oversized indexes
              once the analyser identifies candidates.
            </Typography>
          </CardContent>
        </Card>
      ) : (
        <Grid container spacing={2}>
          {recs.map((rec: any, i: number) => (
            <Grid item xs={12} sm={6} md={4} key={i}>
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: i * 0.05 }}
              >
                <Card>
                  <CardContent>
                    <Box sx={{ display: "flex", justifyContent: "space-between", mb: 1 }}>
                      <Typography variant="subtitle2">
                        {rec.recommendation_type}
                      </Typography>
                      <Chip
                        label={rec.confidence}
                        size="small"
                        sx={{
                          color: confidenceColor[rec.confidence] || "#8B949E",
                          borderColor: confidenceColor[rec.confidence] || "#8B949E",
                        }}
                        variant="outlined"
                      />
                    </Box>
                    <Typography variant="h5" sx={{ mb: 1 }}>
                      {rec.count}
                    </Typography>
                    <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap" }}>
                      <Chip label={`${rec.pending_review} pending`} size="small" color="warning" variant="outlined" />
                      <Chip label={`${rec.approved} approved`} size="small" color="info" variant="outlined" />
                      <Chip label={`${rec.executed} executed`} size="small" color="success" variant="outlined" />
                      {Number(rec.rejected) > 0 && (
                        <Chip label={`${rec.rejected} rejected`} size="small" color="error" variant="outlined" />
                      )}
                    </Box>
                  </CardContent>
                </Card>
              </motion.div>
            </Grid>
          ))}
        </Grid>
      )}
    </Box>
  );
}
